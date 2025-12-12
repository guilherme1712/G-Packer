import os
import time
import random
import threading
import socket
from app.services.auth import get_credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from app.services.Google.drive_tree import get_thread_safe_service

MAX_RETRIES = 5
BASE_DELAY = 1.5
RETRIABLE_STATUS_CODES = [403, 429, 500, 502, 503, 504]

# Lock para criação física de pastas
FOLDER_CREATION_LOCK = threading.Lock()

# CACHE GLOBAL DE PASTAS (RAM)
# Formato: {'parent_id|folder_name': 'new_folder_id'}
# Isso evita que 50 threads batam na API perguntando pela mesma pasta
GLOBAL_FOLDER_CACHE = {}

class DriveUploadService:
    
    @staticmethod
    def execute_with_retry(request_func, *args, **kwargs):
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return request_func(*args, **kwargs).execute()
            except HttpError as e:
                last_error = e
                if e.resp.status not in RETRIABLE_STATUS_CODES: raise e
                time.sleep((BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1))
            except (socket.timeout, ConnectionError) as e:
                last_error = e
                time.sleep((BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1))
        raise last_error

    @staticmethod
    def _internal_retry_execute(request):
        for attempt in range(MAX_RETRIES):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in RETRIABLE_STATUS_CODES:
                    time.sleep((BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1))
                else: raise e
            except (socket.timeout, ConnectionError):
                time.sleep((BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1))
        raise Exception("Falha após retries.")

    @staticmethod
    def upload_single_file_sync(creds, local_path, filename, mimetype, relative_path, root_target_id, user_email):
        if not creds: raise Exception("Credenciais inválidas.")
        if not os.path.exists(local_path): raise FileNotFoundError(f"Temp não encontrado: {local_path}")

        service = get_thread_safe_service(creds)
        final_parent_id = root_target_id
        
        # --- LÓGICA DE CRIAÇÃO DE PASTAS COM CACHE GLOBAL ---
        if relative_path:
            clean_path = relative_path.replace('\\', '/').strip('/')
            
            if '/' in clean_path:
                parts = clean_path.split('/')
                folder_structure = parts[:-1] 
                
                if folder_structure:
                    # Usa o Cache Global
                    final_parent_id = DriveUploadService.ensure_folder_path(
                        creds, root_target_id, folder_structure
                    )

        file_metadata = {'name': filename, 'parents': [final_parent_id]}
        if not mimetype: mimetype = 'application/octet-stream'

        # Upload Resumable (Chunked)
        media = MediaFileUpload(local_path, mimetype=mimetype, resumable=True, chunksize=5*1024*1024)

        try:
            request = service.files().create(body=file_metadata, media_body=media, fields='id, size')
            return DriveUploadService._internal_retry_execute(request)
        except Exception as e:
            print(f"Erro CRÍTICO no upload de {filename}: {str(e)}")
            raise e

    @staticmethod
    def ensure_folder_path(creds, base_parent_id, path_parts):
        """Navega ou cria pastas usando CACHE GLOBAL e LOCK."""
        service = get_thread_safe_service(creds)
        current_parent = base_parent_id
        
        for folder_name in path_parts:
            if not folder_name: continue
            
            cache_key = f"{current_parent}|{folder_name}"
            
            # 1. Tenta Ler do Cache (Rápido, sem Lock)
            if cache_key in GLOBAL_FOLDER_CACHE:
                current_parent = GLOBAL_FOLDER_CACHE[cache_key]
                continue

            # 2. Se não achou, entra na seção crítica
            with FOLDER_CREATION_LOCK:
                # Verifica cache de novo (double-check locking) pois outra thread pode ter criado
                if cache_key in GLOBAL_FOLDER_CACHE:
                    current_parent = GLOBAL_FOLDER_CACHE[cache_key]
                else:
                    # Vai na API do Google
                    existing_id = DriveUploadService.find_folder(service, folder_name, current_parent)
                    if existing_id:
                        current_parent = existing_id
                    else:
                        current_parent = DriveUploadService.create_folder(creds, folder_name, current_parent)
                    
                    # Salva no Cache
                    GLOBAL_FOLDER_CACHE[cache_key] = current_parent

        return current_parent

    @staticmethod
    def create_folder(creds, folder_name, parent_id):
        service = get_thread_safe_service(creds)
        meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        req = service.files().create(body=meta, fields='id')
        return DriveUploadService._internal_retry_execute(req).get('id')

    @staticmethod
    def find_folder(service, name, parent_id):
        q = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        req = service.files().list(q=q, fields="files(id)", pageSize=1)
        res = DriveUploadService._internal_retry_execute(req)
        files = res.get('files', [])
        return files[0]['id'] if files else None

    @staticmethod
    def list_folders(creds, parent_id="root"):
        service = get_thread_safe_service(creds)
        q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        req = service.files().list(q=q, pageSize=100, fields="files(id, name)", orderBy="name")
        return DriveUploadService._internal_retry_execute(req).get('files', [])