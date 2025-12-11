# app/services/Google/drive_ops.py
from datetime import datetime
from googleapiclient.errors import HttpError
from app.models import db, DriveItemCacheModel
from app.services.Google.drive_tree import get_thread_safe_service
from app.services.audit import AuditService

class DriveOperationsService:
    
    @staticmethod
    def rename_file(creds, file_id, new_name):
        service = get_thread_safe_service(creds)
        try:
            # 1. Atualiza no Google Drive
            file = service.files().update(
                fileId=file_id,
                body={'name': new_name},
                fields='id, name, parents, mimeType'
            ).execute()
            
            # 2. Atualiza Cache Local (SQLite)
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.name = new_name
                cache_item.updated_at = datetime.utcnow()
                db.session.commit()
                
            # 3. Auditoria
            AuditService.log("DRIVE_RENAME", new_name, details=f"ID: {file_id}")
            
            return {"ok": True, "file": file}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @staticmethod
    def move_file(creds, file_id, target_folder_id):
        service = get_thread_safe_service(creds)
        try:
            # Recupera pais atuais para remover
            file_meta = service.files().get(fileId=file_id, fields='parents, name').execute()
            previous_parents = ",".join(file_meta.get('parents', []))
            
            # 1. Move no Google Drive
            file = service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=previous_parents,
                fields='id, parents, name'
            ).execute()
            
            # 2. Atualiza Cache Local
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.parent_id = target_folder_id
                # Nota: Recalcular o 'path' completo seria custoso aqui, 
                # deixamos para o próximo sync ou apenas atualizamos o pai lógico.
                cache_item.updated_at = datetime.utcnow()
                db.session.commit()

            AuditService.log("DRIVE_MOVE", file_meta.get('name'), details=f"Para ID: {target_folder_id}")
            
            return {"ok": True}
        except HttpError as e:
            return {"ok": False, "error": f"Erro Google API: {e}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @staticmethod
    def trash_file(creds, file_id):
        service = get_thread_safe_service(creds)
        try:
            # 1. Envia para Lixeira no Drive
            service.files().update(
                fileId=file_id,
                body={'trashed': True}
            ).execute()
            
            # 2. Remove do Cache Local (Marca como trashed ou deleta)
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.trashed = True
                db.session.commit()
            
            AuditService.log("DRIVE_TRASH", f"ID: {file_id}", details="Enviado para lixeira")
            
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @staticmethod
    def create_folder(creds, parent_id, name):
        service = get_thread_safe_service(creds)
        try:
            file_metadata = {
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }

            new_folder = service.files().create(
                body=file_metadata,
                fields="id, name, parents"
            ).execute()

            # Atualiza Cache Local (SQLite)
            new_cache_item = DriveItemCacheModel(
                drive_id=new_folder["id"],
                name=name,
                parent_id=parent_id,
                mime_type="application/vnd.google-apps.folder",
                trashed=False,
            )
            db.session.add(new_cache_item)
            db.session.commit()

            AuditService.log("DRIVE_CREATE_FOLDER", name, details=f"PARENT: {parent_id}")

            return {"ok": True, "folder": new_folder}

        except Exception as e:
            return {"ok": False, "error": str(e)}
