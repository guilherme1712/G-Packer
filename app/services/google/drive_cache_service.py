# app/services/google/drive_cache_service.py
import concurrent.futures
import time
import random
from datetime import datetime
from sqlalchemy.exc import OperationalError

from flask import current_app
from app.models.db_instance import db
from app.models.drive_cache import DriveItemCacheModel
from app.models.task import TaskModel

# REMOVIDOS IMPORTS DO TOPO QUE CAUSAVAM CICLO:
# from app.services.google.drive_tree import get_children
# from app.services.worker_manager import WorkerManager

# Tempo padrão de expiração
DEFAULT_MAX_AGE_SECONDS = 172800 #(24H)

def _ensure_root_item():
    """Garante que a pasta raiz exista no banco."""
    try:
        root = DriveItemCacheModel.query.filter_by(drive_id="root").first()
        if root: return root

        root = DriveItemCacheModel(
            drive_id="root", name="Meu Drive",
            mime_type="application/vnd.google-apps.folder",
            is_folder=True, path="Meu Drive"
        )
        db.session.add(root)
        db.session.commit()
        return root
    except Exception as e:
        db.session.rollback()
        return None


def _upsert_item_from_remote(parent_id, parent_path, item_data, seen_at):
    """
    Insere ou atualiza item no banco.
    """
    try:
        drive_id = item_data["id"]
        name = item_data["name"]
        # Verifica se é pasta pelo mimeType ou type
        is_folder = (item_data.get("type") == "folder") or \
                    (item_data.get("mimeType") == "application/vnd.google-apps.folder")
        
        path = f"{parent_path}/{name}" if parent_path else name

        # Tenta achar existente
        item = DriveItemCacheModel.query.filter_by(drive_id=drive_id).first()
        if not item:
            item = DriveItemCacheModel(drive_id=drive_id)

        # Atualiza campos
        item.name = name
        item.is_folder = is_folder
        item.parent_id = parent_id
        item.path = path
        item.size_bytes = int(item_data.get("size_bytes") or item_data.get("size") or 0)
        item.last_seen_remote = seen_at
        item.trashed = False

        # Novos campos Smart Cleaner
        item.mime_type = item_data.get("mimeType")
        item.md5_checksum = item_data.get("md5Checksum")
        item.modified_time = item_data.get("modified_time") or item_data.get("modifiedTime")

        db.session.add(item)
        return item
    except Exception as e:
        print(f"Erro upsert item {item_data.get('name')}: {e}")
        return None


def _refresh_folder_from_remote(creds, folder_id: str, include_files: bool = True):
    """
    (BLOQUEANTE) Vai no Google, busca filhos e salva no banco.
    """
    # IMPORT LOCAL PARA EVITAR CICLO
    from app.services.google.drive_tree import get_children
    
    _ensure_root_item()

    if folder_id == "root":
        parent_path = "Meu Drive"
    else:
        parent = DriveItemCacheModel.query.filter_by(drive_id=folder_id).first()
        parent_path = parent.path if parent and parent.path else ""

    children = get_children(creds, folder_id, include_files=True)
    seen_at = datetime.utcnow()

    try:
        DriveItemCacheModel.query.filter_by(parent_id=folder_id).delete()
        for child in children:
            _upsert_item_from_remote(folder_id, parent_path, child, seen_at)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(f"Erro refresh remote: {e}")

    return children


def get_children_cached(
        creds,
        folder_id: str,
        include_files: bool = False,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
        force_refresh: bool = False,
):
    """
    Retorna filhos do cache.
    """
    _ensure_root_item()

    if force_refresh:
        children = _refresh_folder_from_remote(creds, folder_id, include_files=True)
        result = []
        for child in children:
            if child["type"] == "file" and not include_files:
                continue
            result.append({
                "id": child["id"],
                "name": child["name"],
                "type": child["type"],
                "size_bytes": child.get("size_bytes"),
            })
        return result

    # MODO LEITURA (READ-ONLY)
    query = DriveItemCacheModel.query.filter_by(parent_id=folder_id, trashed=False)
    query = query.order_by(DriveItemCacheModel.is_folder.desc(), DriveItemCacheModel.name.asc())
    cached_children = query.all()

    return [
        c.to_tree_node()
        for c in cached_children
        if include_files or c.is_folder
    ]


def search_cache(
        text: str | None = None,
        type_filter: str | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        limit: int = 200,
) -> list[dict]:
    query = DriveItemCacheModel.query.filter_by(trashed=False)

    if text:
        like = f"%{text}%"
        query = query.filter(DriveItemCacheModel.name.ilike(like))

    if type_filter == "file":
        query = query.filter(DriveItemCacheModel.is_folder.is_(False))
    elif type_filter == "folder":
        query = query.filter(DriveItemCacheModel.is_folder.is_(True))

    if min_size is not None:
        query = query.filter(DriveItemCacheModel.size_bytes >= min_size)
    if max_size is not None:
        query = query.filter(DriveItemCacheModel.size_bytes <= max_size)

    query = query.order_by(DriveItemCacheModel.path.asc()).limit(limit)
    items = query.all()
    return [i.to_dict() for i in items]


def cache_uploaded_item(creds, file_id: str):
    """
    Atualiza UM ÚNICO item no cache após upload.
    """
    try:
        # IMPORT LOCAL
        from app.services.google.drive_tree import get_thread_safe_service
        service = get_thread_safe_service(creds)

        # 1. Busca metadados
        meta = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, parents, modifiedTime, md5Checksum"
        ).execute()

        parent_id = meta.get('parents', ['root'])[0]

        # 2. Descobre o path do pai
        parent_cache = DriveItemCacheModel.query.filter_by(drive_id=parent_id).first()

        if parent_cache and parent_cache.path:
            parent_path = parent_cache.path
        elif parent_id == 'root':
            parent_path = "Meu Drive"
        else:
            parent_path = "" 

        # 3. Prepara dados
        item_data = {
            "id": meta["id"],
            "name": meta["name"],
            "type": "folder" if meta["mimeType"] == "application/vnd.google-apps.folder" else "file",
            "mimeType": meta["mimeType"],
            "size_bytes": meta.get("size", 0),
            "modified_time": meta.get("modifiedTime"),
            "md5Checksum": meta.get("md5Checksum")
        }

        _ensure_root_item()

        # 4. Insere/Atualiza
        upserted = _upsert_item_from_remote(
            parent_id=parent_id,
            parent_path=parent_path,
            item_data=item_data,
            seen_at=datetime.utcnow()
        )

        db.session.commit()
        return upserted

    except Exception as e:
        db.session.rollback()
        print(f"[CACHE] Erro cache upload item {file_id}: {e}")
        return None


# ==========================================
# WORKER DE BACKGROUND
# ==========================================
def _worker_process_folder_cache(app, creds, folder_id, folder_path, include_files):
    # IMPORT LOCAL
    from app.services.google.drive_tree import get_children
    
    subfolders_found = []
    items_count = 0
    max_retries = 3

    with app.app_context():
        for attempt in range(max_retries):
            try:
                children = get_children(creds, folder_id, include_files=include_files)
                seen_at = datetime.utcnow()

                for child in children:
                    item = _upsert_item_from_remote(folder_id, folder_path, child, seen_at)
                    if item:
                        items_count += 1
                        if item.is_folder:
                            subfolders_found.append((item.drive_id, item.path))

                db.session.commit()
                break

            except OperationalError as e:
                db.session.rollback()
                if "locked" in str(e):
                    time.sleep(random.uniform(0.2, 1.0))
                    continue
                else:
                    print(f">>> [Worker Error] {folder_path}: {e}")
                    break
            except Exception as e:
                db.session.rollback()
                print(f">>> [Worker Fatal] {folder_path}: {e}")
                break

        db.session.remove()

    return subfolders_found, items_count


def rebuild_full_cache(creds, include_files: bool = True, task_id: str = None) -> int:
    """
    Função principal chamada pela thread de Login.
    """
    # IMPORT LOCAL PARA EVITAR CICLO
    from app.services.worker_manager import WorkerManager
    
    db.session.remove()

    if task_id:
        try:
            t = TaskModel.query.get(task_id)
            if t:
                t.phase = "mapeando"
                t.message = "Iniciando leitura do Drive..."
                t.files_found = 0
                db.session.commit()
        except:
            db.session.rollback()

    total_items = 0

    try:
        root = _ensure_root_item()
        if not root: raise Exception("Root DB error")

        executor = WorkerManager.get_mapping_executor()
        app_obj = current_app._get_current_object()

        future_to_path = {}
        fut = executor.submit(_worker_process_folder_cache, app_obj, creds, "root", root.path, include_files)
        future_to_path[fut] = "root"

        while future_to_path:
            done, _ = concurrent.futures.wait(
                future_to_path.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future in done:
                path_processed = future_to_path.pop(future)
                try:
                    subs, count = future.result()
                    total_items += count
                    print(f" -> Mapeado: {path_processed} ({count} itens)")

                    if task_id:
                        try:
                            t = TaskModel.query.get(task_id)
                            t.files_found = total_items
                            t.files_downloaded = total_items
                            t.message = f"Lendo: {path_processed[:40]}..."
                            db.session.commit()
                        except:
                            db.session.rollback()

                    for sid, spath in subs:
                        nf = executor.submit(_worker_process_folder_cache, app_obj, creds, sid, spath, include_files)
                        future_to_path[nf] = spath

                except Exception as e:
                    print(f"Erro path {path_processed}: {e}")

        if task_id:
            try:
                t = TaskModel.query.get(task_id)
                t.phase = "concluido"
                t.message = f"Mapeamento OK: {total_items} itens."
                t.files_total = total_items
                db.session.commit()
            except:
                db.session.rollback()

    except Exception as e:
        print(f"Erro Fatal Rebuild: {e}")
        if task_id:
            try:
                t = TaskModel.query.get(task_id)
                t.phase = "erro"
                t.message = str(e)
                db.session.commit()
            except:
                db.session.rollback()

    return total_items