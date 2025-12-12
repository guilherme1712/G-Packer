import os
import time
import random
import threading
import socket
from app.services.auth import get_credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from app.services.Google.drive_tree import get_thread_safe_service
from app.models import db, UploadHistoryModel, TaskModel

# =========================================================================
# CONSTANTES DE RETRY (CONFIGURAÇÃO)
# =========================================================================
MAX_RETRIES = 5  # Tenta 5 vezes antes de desistir
BASE_DELAY = 1.5 # Segundos iniciais de espera (multiplica por 2 a cada erro)

# Códigos HTTP que merecem retry (403/429 = Rate Limit, 5xx = Erro no Google)
RETRIABLE_STATUS_CODES = [403, 429, 500, 502, 503, 504]

# Lock global para evitar duplicidade de pastas durante upload paralelo
FOLDER_CREATION_LOCK = threading.Lock()

# Armazena progresso em memória (Legado/Compatibilidade)
UPLOAD_PROGRESS = {}

class DriveUploadService:
    
    @staticmethod
    def execute_with_retry(request_func, *args, **kwargs):
        """
        Executa uma função da API do Google com lógica de Retry Exponencial.
        Usado para evitar falhas por instabilidade de rede ou Rate Limit.
        """
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                # Executa a requisição
                return request_func(*args, **kwargs).execute()
            
            except HttpError as e:
                last_error = e
                # Erros fatais (400, 401, 404) não adiantam retentar
                if e.resp.status not in RETRIABLE_STATUS_CODES:
                    raise e
                
                # Rate Limit ou Server Error -> Backoff Exponencial
                sleep_time = (BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1)
                print(f"[DriveUpload] Erro {e.resp.status}. Tentativa {attempt+1}/{MAX_RETRIES}. Esperando {sleep_time:.2f}s...")
                time.sleep(sleep_time)

            except (socket.timeout, ConnectionError) as e:
                last_error = e
                sleep_time = (BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1)
                print(f"[DriveUpload] Erro de Conexão. Tentativa {attempt+1}/{MAX_RETRIES}. Esperando {sleep_time:.2f}s...")
                time.sleep(sleep_time)

        print(f"[DriveUpload] Falha definitiva após {MAX_RETRIES} tentativas.")
        raise last_error

    @staticmethod
    def upload_single_file_sync(creds, local_path, filename, mimetype, relative_path, root_target_id, user_email):
        """
        Realiza o upload síncrono com RETRY automático em caso de falha.
        Retorna o objeto 'file' do Google Drive (dict) com 'id' e 'size'.
        """
        if not creds:
            raise Exception("Credenciais inválidas fornecidas para o upload.")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Arquivo temporário não encontrado: {local_path}")

        service = get_thread_safe_service(creds)

        # 1. Resolver Estrutura de Pastas
        final_parent_id = root_target_id
        
        if relative_path and relative_path != filename:
            clean_path = relative_path.replace('\\', '/').strip('/')
            parts = clean_path.split('/')
            folder_structure = parts[:-1] # Remove nome do arquivo
            
            if folder_structure:
                dummy_cache = {} 
                final_parent_id = DriveUploadService.ensure_folder_path(
                    creds, root_target_id, folder_structure, dummy_cache
                )

        # 2. Preparar Metadados
        file_metadata = {
            'name': filename,
            'parents': [final_parent_id]
        }
        
        if not mimetype:
            mimetype = 'application/octet-stream'

        # 3. Executar Upload (COM RETRY)
        media = MediaFileUpload(local_path, mimetype=mimetype, resumable=True, chunksize=5*1024*1024)

        try:
            request = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, size'
            )
            
            file = DriveUploadService._internal_retry_execute(request)
            return file

        except Exception as e:
            print(f"Erro CRÍTICO no upload de {filename}: {str(e)}")
            raise e

    @staticmethod
    def _internal_retry_execute(request):
        """Helper para executar objetos Request criados manualmente."""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in RETRIABLE_STATUS_CODES:
                    sleep_time = (BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    raise e
            except (socket.timeout, ConnectionError):
                sleep_time = (BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1)
                time.sleep(sleep_time)
                
        raise Exception(f"Falha no upload após {MAX_RETRIES} tentativas.")

    @staticmethod
    def ensure_folder_path(creds, base_parent_id, path_parts, cache):
        """Navega ou cria pastas com Lock para thread-safety."""
        service = get_thread_safe_service(creds)
        current_parent = base_parent_id
        
        for folder_name in path_parts:
            if not folder_name: continue
            
            cache_key = f"{current_parent}|{folder_name}"
            if cache and cache_key in cache:
                current_parent = cache[cache_key]
                continue

            # --- SEÇÃO CRÍTICA (LOCK) ---
            with FOLDER_CREATION_LOCK:
                existing_id = DriveUploadService.find_folder(service, folder_name, current_parent)
                
                if existing_id:
                    current_parent = existing_id
                else:
                    current_parent = DriveUploadService.create_folder(creds, folder_name, current_parent)
            # --- FIM SEÇÃO CRÍTICA ---

            if cache is not None:
                cache[cache_key] = current_parent
                
        return current_parent

    @staticmethod
    def create_folder(creds, folder_name, parent_id="root"):
        service = get_thread_safe_service(creds)
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        request = service.files().create(body=file_metadata, fields='id')
        file = DriveUploadService._internal_retry_execute(request)
        return file.get('id')

    @staticmethod
    def find_folder(service, name, parent_id):
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        request = service.files().list(q=query, fields="files(id)", pageSize=1)
        results = DriveUploadService._internal_retry_execute(request)
        files = results.get('files', [])
        return files[0]['id'] if files else None
        
    @staticmethod
    def list_folders(creds, parent_id="root"):
        service = get_thread_safe_service(creds)
        query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        request = service.files().list(q=query, pageSize=100, fields="files(id, name)", orderBy="name")
        results = DriveUploadService._internal_retry_execute(request)
        return results.get('files', [])

    # --- Worker de Upload (Core da Lógica de Batch) ---
    @staticmethod
    def _upload_worker(app, upload_history_id):
        """
        Executa o upload em background e atualiza o Status da Task Pai (Batch).
        """
        with app.app_context():
            upload_record = db.session.get(UploadHistoryModel, upload_history_id)
            if not upload_record:
                return

            try:
                creds = get_credentials()
                if not creds:
                    raise Exception("Sem credenciais válidas na thread.")

                if not upload_record.temp_path or not os.path.exists(upload_record.temp_path):
                    raise FileNotFoundError(f"Arquivo temporário sumiu: {upload_record.temp_path}")

                # EXECUTA O UPLOAD REAL
                result = DriveUploadService.upload_single_file_sync(
                    creds=creds,
                    local_path=upload_record.temp_path,
                    filename=upload_record.filename,
                    mimetype=upload_record.mime_type,
                    relative_path=upload_record.relative_path,
                    root_target_id=upload_record.destination_id,
                    user_email=upload_record.user_email
                )

                # Sucesso: Atualiza o registro individual
                upload_record.status = 'SUCCESS'
                upload_record.file_id = result.get('id')
                upload_record.size_bytes = int(result.get('size', 0))

                # Remove arquivo local
                if os.path.exists(upload_record.temp_path):
                    os.remove(upload_record.temp_path)

            except Exception as e:
                print(f"[Worker Error] ID {upload_history_id} ({upload_record.filename}): {e}")
                upload_record.status = 'ERROR'
                upload_record.error_message = str(e)

            finally:
                # --- ATUALIZAÇÃO DA TASK PAI (LOTE) ---
                try:
                    if upload_record.task_id:
                        # Carrega a Task Pai (Lote)
                        parent_task = db.session.get(TaskModel, upload_record.task_id)
                        
                        if parent_task:
                            # Incremento Atômico (+1) para garantir thread-safety no banco
                            # Isso evita que duas threads sobrescrevam o contador uma da outra
                            if upload_record.status == 'SUCCESS':
                                parent_task.files_downloaded = TaskModel.files_downloaded + 1
                                parent_task.bytes_downloaded = TaskModel.bytes_downloaded + upload_record.size_bytes
                            elif upload_record.status == 'ERROR':
                                parent_task.errors_count = TaskModel.errors_count + 1

                            # Opcional: Atualizar mensagem de status se quiser log no banco
                            # (Mas o front usa o contador numérico, então isso é apenas cosmético)
                
                    db.session.commit()

                except Exception as db_err:
                    print(f"[Worker DB Error] Falha ao salvar status final: {db_err}")
                    db.session.rollback()

    # --- Métodos Legados ---
    @staticmethod
    def get_progress(task_id):
        return UPLOAD_PROGRESS.get(task_id, {})

    @staticmethod
    def cancel_task(task_id):
        if task_id in UPLOAD_PROGRESS:
            UPLOAD_PROGRESS[task_id]['cancel'] = True
            return True
        return False