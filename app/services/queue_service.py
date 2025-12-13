import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor
from app.models import db, UploadHistoryModel, TaskModel
from app.services.google.drive_upload import DriveUploadService
from app.services.worker_manager import WorkerManager
from app.services.google.drive_cache_service import cache_uploaded_item
from app.services.auth import get_credentials

# Usamos o limite configurado no WorkerManager
MAX_CONCURRENT_UPLOADS = WorkerManager.MAX_UPLOAD_WORKERS

bg_thread = None
keep_running = True

class QueueService:
    @staticmethod
    def start_worker(app):
        global bg_thread
        if bg_thread is None or not bg_thread.is_alive():
            bg_thread = threading.Thread(target=QueueService._manager_loop, args=(app,))
            bg_thread.daemon = True
            bg_thread.start()
            print(f"[QueueService] Manager iniciado. Capacidade: {MAX_CONCURRENT_UPLOADS} threads.")

    @staticmethod
    def _manager_loop(app):
        # Obtém o executor compartilhado (Pool de Threads)
        executor = WorkerManager.get_upload_executor()
        active_futures = []

        with app.app_context():
            print("[QueueService] Loop de monitoramento rodando...")
            while keep_running:
                try:
                    # Limpa futures já concluídos da lista local
                    active_futures = [f for f in active_futures if not f.done()]
                    
                    # Calcula quantos slots temos livres no WorkerManager
                    slots_available = MAX_CONCURRENT_UPLOADS - len(active_futures)

                    if slots_available > 0:
                        # Busca tarefas PENDENTES no limite dos slots vazios
                        tasks = UploadHistoryModel.query.filter_by(status='PENDING')\
                            .order_by(UploadHistoryModel.id.asc())\
                            .limit(slots_available)\
                            .all()
                        
                        if tasks:
                            print(f"[QueueService] Despachando {len(tasks)} tarefas para threads...")
                            
                            # Atualiza status em lote (Performance DB)
                            for task in tasks:
                                task.status = 'UPLOADING'
                            db.session.commit()

                            # Submete cada tarefa para uma thread do WorkerManager
                            for task in tasks:
                                future = executor.submit(QueueService._upload_worker, app, task.id)
                                active_futures.append(future)
                        else:
                            # Se não tem tarefas, dorme um pouco para não fritar a CPU
                            time.sleep(1.0)
                    else:
                        # Se fila cheia, espera liberar slot
                        time.sleep(0.5)
                
                except Exception as e:
                    print(f"[QueueService] Erro no Manager Loop: {e}")
                    time.sleep(5)

    @staticmethod
    def _upload_worker(app, task_id):
        """Executado por uma Thread do WorkerManager."""
        with app.app_context():
            # DB Session Scoped é criada automaticamente aqui
            upload_record = db.session.get(UploadHistoryModel, task_id)
            if not upload_record: return

            try:
                creds = get_credentials()
                if not creds: raise Exception("Sem credenciais.")
                
                if not upload_record.temp_path or not os.path.exists(upload_record.temp_path):
                    raise FileNotFoundError("Arquivo temporário não encontrado.")

                # UPLOAD (Usa o Cache de Pastas novo)
                result = DriveUploadService.upload_single_file_sync(
                    creds=creds,
                    local_path=upload_record.temp_path,
                    filename=upload_record.filename,
                    mimetype=upload_record.mime_type,
                    relative_path=upload_record.relative_path, 
                    root_target_id=upload_record.destination_id,
                    user_email=upload_record.user_email
                )

                upload_record.status = 'SUCCESS'
                upload_record.file_id = result.get('id')
                upload_record.size_bytes = int(result.get('size', 0))
                
                if upload_record.file_id:
                    cache_uploaded_item(creds, upload_record.file_id)
                
                # Limpa disco
                try:
                    if os.path.exists(upload_record.temp_path): os.remove(upload_record.temp_path)
                except: pass

            except Exception as e:
                print(f"[Thread-{task_id}] Erro: {e}")
                upload_record.status = 'ERROR'
                upload_record.error_message = str(e)
            
            finally:
                # Atualização Atômica da Task Pai
                try:
                    if upload_record.task_id:
                        parent_task = db.session.get(TaskModel, upload_record.task_id)
                        if parent_task:
                            if upload_record.status == 'SUCCESS':
                                parent_task.files_downloaded = (parent_task.files_downloaded or 0) + 1
                                parent_task.bytes_downloaded = (parent_task.bytes_downloaded or 0) + upload_record.size_bytes
                            elif upload_record.status == 'ERROR':
                                parent_task.errors_count = (parent_task.errors_count or 0) + 1
                    
                    db.session.commit()
                except Exception as db_err:
                    print(f"[Thread-{task_id}] Erro DB Update: {db_err}")
                    db.session.rollback()

    @staticmethod
    def get_global_progress():
        return {}