# app/services/Google/drive_upload.py
from googleapiclient.http import MediaIoBaseUpload
from app.services.Google.drive_tree import get_thread_safe_service

class DriveUploadService:
    @staticmethod
    def upload_file(creds, file_obj, filename, parent_id="root", mimetype=None):
        service = get_thread_safe_service(creds)
        if not mimetype: mimetype = "application/octet-stream"

        file_metadata = {'name': filename, 'parents': [parent_id]}
        media = MediaIoBaseUpload(file_obj, mimetype=mimetype, resumable=True, chunksize=1024*1024*5)

        return service.files().create(
            body=file_metadata, media_body=media, fields='id, size, name'
        ).execute()

    @staticmethod
    def create_folder(creds, folder_name, parent_id="root"):
        service = get_thread_safe_service(creds)
        meta = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        return service.files().create(body=meta, fields='id').execute().get('id')

    @staticmethod
    def find_folder(service, name, parent_id):
        q = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
        files = res.get('files', [])
        return files[0]['id'] if files else None

    @staticmethod
    def ensure_folder_path(creds, base_parent_id, path_parts, cache):
        service = get_thread_safe_service(creds)
        current = base_parent_id
        for name in path_parts:
            key = f"{current}|{name}"
            if key in cache:
                current = cache[key]
            else:
                fid = DriveUploadService.find_folder(service, name, current)
                if not fid:
                    fid = DriveUploadService.create_folder(creds, name, current)
                current = fid
                cache[key] = current
        return current
