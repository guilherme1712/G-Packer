import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor
from app.models import db, UploadHistoryModel
from app.services.Google.drive_upload import DriveUploadService
from app.services.worker_manager import WorkerManager
from app.services.auth import get_credentials

# =========================================================================
# CONSTANTE DE PARALELISMO
# Define quantos arquivos serão enviados SIMULTANEAMENTE para o Drive.
# =========================================================================
MAX_CONCURRENT_UPLOADS = 150

# Variáveis globais de controle
bg_thread = None
keep_running = True

class QueueService:
    @staticmethod
    def start_worker(app):
        """Inicia o gerenciador de fila se não estiver rodando."""
        global bg_thread
        if bg_thread is None or not bg_thread.is_alive():
            bg_thread = threading.Thread(target=QueueService._manager_loop, args=(app,))
            bg_thread.daemon = True
            bg_thread.start()
            print(f"[QueueService] Gerenciador iniciado com {MAX_CONCURRENT_UPLOADS} threads paralelas.")

    @staticmethod
    def _manager_loop(app):
        """
        Loop principal que gerencia a distribuição de tarefas
        usando o Pool Central de Uploads.
        """
        # SUBSTUIÇÃO: Em vez de criar um executor novo, pegamos o global
        executor = WorkerManager.get_upload_executor()
        
        # Pega o limite configurado no Manager
        max_workers = WorkerManager.MAX_UPLOAD_WORKERS
        
        active_futures = []

        with app.app_context():
            print("[QueueService] Loop de monitoramento ativo (WorkerManager)...")
            while keep_running:
                try:
                    active_futures = [f for f in active_futures if not f.done()]

                    slots_available = max_workers - len(active_futures)

                    if slots_available > 0:
                        tasks = UploadHistoryModel.query.filter_by(status='PENDING')\
                            .order_by(UploadHistoryModel.id.asc())\
                            .limit(slots_available)\
                            .all()
                        
                        if tasks:
                            for task in tasks:
                                task.status = 'UPLOADING'
                                db.session.commit()

                                future = executor.submit(QueueService._upload_worker, app, task.id)
                                active_futures.append(future)
                        else:
                            time.sleep(1.5)
                    else:
                        time.sleep(0.5)
                
                except Exception as e:
                    print(f"[QueueService] Erro: {e}")
                    time.sleep(5)

    @staticmethod
    def _upload_worker(app, task_id):
        """
        Esta função roda DENTRO de cada thread individualmente.
        Faz o upload pesado e atualiza o banco.
        """
        # Cada thread precisa do seu próprio contexto de aplicação e sessão de banco
        with app.app_context():
            # Busca a task novamente (nova sessão do DB para esta thread)
            task = db.session.get(UploadHistoryModel, task_id)
            if not task: return

            try:
                creds = get_credentials()
                if not creds:
                    raise Exception("Sem credenciais válidas na thread.")

                # Verifica arquivo físico
                if not task.temp_path or not os.path.exists(task.temp_path):
                    raise FileNotFoundError(f"Arquivo temporário sumiu: {task.temp_path}")

                # --- UPLOAD REAL (O Retry está dentro desta função agora) ---
                result = DriveUploadService.upload_single_file_sync(
                    creds=creds,
                    local_path=task.temp_path,
                    filename=task.filename,
                    mimetype=task.mime_type,
                    relative_path=task.relative_path,
                    root_target_id=task.destination_id,
                    user_email=task.user_email
                )

                # Sucesso
                task.status = 'SUCCESS'
                task.file_id = result.get('id')
                task.size_bytes = int(result.get('size', 0))
                
                # Remove arquivo temp com segurança
                try:
                    if os.path.exists(task.temp_path):
                        os.remove(task.temp_path)
                except Exception as clean_err:
                    print(f"[Worker Warning] Falha ao limpar temp {task_id}: {clean_err}")

            except Exception as e:
                # Se chegou aqui, é porque os 5 retries falharam ou é um erro fatal
                print(f"[Worker Error] ID {task_id} ({task.filename}): {e}")
                task.status = 'ERROR'
                task.error_message = str(e)
            
            finally:
                # Commit final da thread para salvar SUCCESS ou ERROR
                try:
                    db.session.commit()
                except Exception as db_err:
                    print(f"[Worker DB Error] Falha ao salvar status final: {db_err}")
                    db.session.rollback()

    @staticmethod
    def get_global_progress():
        """Retorna estatísticas para o Frontend."""
        try:
            # Queries otimizadas apenas para contagem
            total_q = UploadHistoryModel.query.count()
            
            # PENDING e UPLOADING contam como "Fila Ativa"
            pending = UploadHistoryModel.query.filter(
                UploadHistoryModel.status.in_(['PENDING', 'UPLOADING'])
            ).count()
            
            success = UploadHistoryModel.query.filter_by(status='SUCCESS').count()
            error = UploadHistoryModel.query.filter_by(status='ERROR').count()
            
            return {
                "total_files": total_q,
                "pending": pending,
                "success": success,
                "error": error,
                "is_active": pending > 0
            }
        except Exception as e:
            # Fallback em caso de erro no banco (ex: lock)
            print(f"[Status Error] {e}")
            return {"total_files": 0, "pending": 0, "success": 0, "error": 0, "is_active": False}