import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor
from app.models import db, UploadHistoryModel, TaskModel  # <--- IMPORTANTE: Importar TaskModel
from app.services.Google.drive_upload import DriveUploadService
from app.services.worker_manager import WorkerManager
from app.services.auth import get_credentials

# =========================================================================
# CONSTANTE DE PARALELISMO
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
        """Loop principal que gerencia a distribuição de tarefas."""
        executor = WorkerManager.get_upload_executor()
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
                                db.session.commit() # Commit rápido para marcar como UPLOADING
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
        Faz o upload pesado e atualiza o banco (Arquivo E Task Pai).
        """
        with app.app_context():
            # Busca o registro do arquivo
            upload_record = db.session.get(UploadHistoryModel, task_id)
            if not upload_record: return

            try:
                creds = get_credentials()
                if not creds: raise Exception("Sem credenciais válidas na thread.")

                if not upload_record.temp_path or not os.path.exists(upload_record.temp_path):
                    raise FileNotFoundError(f"Arquivo temporário sumiu: {upload_record.temp_path}")

                # --- UPLOAD REAL ---
                result = DriveUploadService.upload_single_file_sync(
                    creds=creds,
                    local_path=upload_record.temp_path,
                    filename=upload_record.filename,
                    mimetype=upload_record.mime_type,
                    relative_path=upload_record.relative_path, # Já tratado no upload.py
                    root_target_id=upload_record.destination_id,
                    user_email=upload_record.user_email
                )

                # Sucesso
                upload_record.status = 'SUCCESS'
                upload_record.file_id = result.get('id')
                upload_record.size_bytes = int(result.get('size', 0))
                
                if os.path.exists(upload_record.temp_path):
                    os.remove(upload_record.temp_path)

            except Exception as e:
                print(f"[Worker Error] ID {task_id} ({upload_record.filename}): {e}")
                upload_record.status = 'ERROR'
                upload_record.error_message = str(e)
            
            finally:
                # ==============================================================================
                # CORREÇÃO DO PROGRESSO: ATUALIZAR A TASK PAI (LOTE)
                # ==============================================================================
                try:
                    if upload_record.task_id:
                        parent_task = db.session.get(TaskModel, upload_record.task_id)
                        if parent_task:
                            # Incremento Atômico (+1) direto no banco para evitar conflito de threads
                            if upload_record.status == 'SUCCESS':
                                parent_task.files_downloaded = TaskModel.files_downloaded + 1
                                parent_task.bytes_downloaded = TaskModel.bytes_downloaded + upload_record.size_bytes
                            elif upload_record.status == 'ERROR':
                                parent_task.errors_count = TaskModel.errors_count + 1
                    
                    # Salva tudo (Status do Arquivo + Incremento da Task) em uma única transação
                    db.session.commit()

                except Exception as db_err:
                    print(f"[Worker DB Error] Falha ao salvar status final: {db_err}")
                    db.session.rollback()

    @staticmethod
    def get_global_progress():
        """Mantido para compatibilidade, mas o Frontend agora usa o endpoint batch-status."""
        return {}