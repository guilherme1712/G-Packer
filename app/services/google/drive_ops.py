# app/services/google/drive_ops.py
from datetime import datetime
from googleapiclient.errors import HttpError
from app.models import db, DriveItemCacheModel
from app.services.google.drive_tree import get_thread_safe_service
from app.services.audit import AuditService


class DriveOperationsService:

    @staticmethod
    def _get_parent_path(parent_id):
        """Helper local para descobrir o caminho do pai sem chamar API."""
        if not parent_id or parent_id == 'root':
            return "Meu Drive"

        parent = DriveItemCacheModel.query.filter_by(drive_id=parent_id).first()
        if parent and parent.path:
            return parent.path
        return ""

    @staticmethod
    def rename_file(creds, file_id, new_name):
        service = get_thread_safe_service(creds)
        try:
            # 1. Atualiza no Google Drive API
            file = service.files().update(
                fileId=file_id,
                body={'name': new_name},
                fields='id, name, parents, mimeType, modifiedTime'
            ).execute()

            # 2. Atualiza Cache Local (Apenas o Node)
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.name = new_name
                # Recalcula o path visual (se tiver pai)
                parent_path = DriveOperationsService._get_parent_path(cache_item.parent_id)
                if parent_path:
                    cache_item.path = f"{parent_path}/{new_name}"
                else:
                    cache_item.path = new_name

                cache_item.updated_at = datetime.utcnow()
                cache_item.last_seen_remote = datetime.utcnow()
                db.session.commit()

            # 3. Auditoria
            AuditService.log("DRIVE_RENAME", new_name, details=f"ID: {file_id}")

            return {"ok": True, "file": file}
        except Exception as e:
            db.session.rollback()
            return {"ok": False, "error": str(e)}

    @staticmethod
    def move_file(creds, file_id, target_folder_id):
        service = get_thread_safe_service(creds)
        try:
            # Recupera pais atuais para remover
            file_meta = service.files().get(fileId=file_id, fields='parents, name').execute()
            previous_parents = ",".join(file_meta.get('parents', []))

            # 1. Move no Google Drive API
            file = service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=previous_parents,
                fields='id, parents, name'
            ).execute()

            # 2. Atualiza Cache Local (Apenas o Node)
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.parent_id = target_folder_id

                # Recalcula o path baseado no NOVO pai (sem refresh recursivo)
                parent_path = DriveOperationsService._get_parent_path(target_folder_id)
                if parent_path:
                    cache_item.path = f"{parent_path}/{cache_item.name}"

                cache_item.updated_at = datetime.utcnow()
                cache_item.last_seen_remote = datetime.utcnow()
                db.session.commit()

            AuditService.log("DRIVE_MOVE", file_meta.get('name'), details=f"Para ID: {target_folder_id}")

            return {"ok": True}
        except HttpError as e:
            return {"ok": False, "error": f"Erro Google API: {e}"}
        except Exception as e:
            db.session.rollback()
            return {"ok": False, "error": str(e)}

    @staticmethod
    def trash_file(creds, file_id):
        service = get_thread_safe_service(creds)
        try:
            # 1. Envia para Lixeira no Drive API
            service.files().update(
                fileId=file_id,
                body={'trashed': True}
            ).execute()

            # 2. Atualiza Cache Local (Soft Delete)
            cache_item = DriveItemCacheModel.query.filter_by(drive_id=file_id).first()
            if cache_item:
                cache_item.trashed = True
                cache_item.updated_at = datetime.utcnow()
                db.session.commit()

            AuditService.log("DRIVE_TRASH", f"ID: {file_id}", details="Enviado para lixeira")

            return {"ok": True}
        except Exception as e:
            db.session.rollback()
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

            # 1. Cria na API
            new_folder = service.files().create(
                body=file_metadata,
                fields="id, name, parents, createdTime, modifiedTime"
            ).execute()

            # 2. Insere no Cache Local imediatamente
            parent_path = DriveOperationsService._get_parent_path(parent_id)
            full_path = f"{parent_path}/{name}" if parent_path else name

            new_cache_item = DriveItemCacheModel(
                drive_id=new_folder["id"],
                name=name,
                parent_id=parent_id,
                path=full_path,
                mime_type="application/vnd.google-apps.folder",
                is_folder=True,
                size_bytes=0,
                trashed=False,
                last_seen_remote=datetime.utcnow(),
                # created_at e updated_at são automáticos do DB, mas podemos forçar se vier do Drive
                modified_time=new_folder.get("modifiedTime")
            )
            db.session.add(new_cache_item)
            db.session.commit()

            AuditService.log("DRIVE_CREATE_FOLDER", name, details=f"PARENT: {parent_id}")

            # Retorna formato compatível com o frontend para adicionar na árvore sem reload
            return {
                "ok": True,
                "folder": {
                    "id": new_folder["id"],
                    "name": name,
                    "type": "folder"
                }
            }

        except Exception as e:
            db.session.rollback()
            return {"ok": False, "error": str(e)}