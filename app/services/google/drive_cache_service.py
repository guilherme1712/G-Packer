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
from app.services.google.drive_tree import get_children
from app.services.worker_manager import WorkerManager

# Tempo padrão de expiração (não usado no modo Read-Only, mas mantido para referência)
DEFAULT_MAX_AGE_SECONDS = 172800


def _ensure_root_item():
    """Garante que a pasta raiz exista no banco para evitar FK errors."""
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
        # Não printa erro se for apenas concorrência de criação
        return None


def _upsert_item_from_remote(parent_id, parent_path, item_data, seen_at):
    """
    Insere ou atualiza item no banco (Usado pelos Workers).
    Não faz commit, apenas adiciona à sessão.
    """
    try:
        drive_id = item_data["id"]
        name = item_data["name"]
        is_folder = (item_data["type"] == "folder")
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
        item.size_bytes = int(item_data.get("size_bytes") or 0)
        item.last_seen_remote = seen_at
        item.trashed = False

        db.session.add(item)
        return item
    except Exception as e:
        print(f"Erro upsert item {item_data.get('name')}: {e}")
        return None


def _refresh_folder_from_remote(creds, folder_id: str, include_files: bool = True):
    """
    (BLOQUEANTE) Vai no Google, busca filhos e salva no banco.
    Usado apenas se force_refresh=True.
    """
    _ensure_root_item()

    if folder_id == "root":
        parent_path = "Meu Drive"
    else:
        parent = DriveItemCacheModel.query.filter_by(drive_id=folder_id).first()
        parent_path = parent.path if parent and parent.path else ""

    children = get_children(creds, folder_id, include_files=True)
    seen_at = datetime.utcnow()

    # Remove antigos para garantir sincronia limpa
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
    MODO LEITURA: Prioriza o banco de dados. Não chama API do Google se não forçado.
    """
    # 1. Garante Root (Leitura rápida)
    _ensure_root_item()

    # 2. SE FORÇADO: Executa a operação de escrita (Cuidado com Locks)
    if force_refresh:
        children = _refresh_folder_from_remote(creds, folder_id, include_files=True)
        # Converte dicionários puros para formato tree node
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

    # 3. MODO PADRÃO: APENAS LEITURA (READ-ONLY)
    # Confia que a Task de Background está populando os dados.
    # Isso evita "Database Locked" durante a navegação.

    query = DriveItemCacheModel.query.filter_by(parent_id=folder_id, trashed=False)

    # Ordenação: Pastas primeiro, depois ordem alfabética
    query = query.order_by(DriveItemCacheModel.is_folder.desc(), DriveItemCacheModel.name.asc())

    cached_children = query.all()

    # Converte retorno do banco para JSON da árvore
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
    """Busca apenas no banco local (Rápido)"""
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
    """Atualiza um único item no cache após upload (Helper)."""
    # (Mantido igual, mas com rollback preventivo)
    try:
        from app.services.google.drive_tree import get_thread_safe_service
        service = get_thread_safe_service(creds)

        meta = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, parents, modifiedTime, createdTime"
        ).execute()

        parent_id = meta.get('parents', ['root'])[0]

        # Tenta descobrir path do pai
        parent_cache = DriveItemCacheModel.query.filter_by(drive_id=parent_id).first()
        parent_path = parent_cache.path if parent_cache else ("Meu Drive" if parent_id == 'root' else "")

        item_data = {
            "id": meta["id"],
            "name": meta["name"],
            "type": "folder" if meta["mimeType"] == "application/vnd.google-apps.folder" else "file",
            "mimeType": meta["mimeType"],
            "size_bytes": meta.get("size", 0),
            "modified_time": meta.get("modifiedTime")
        }

        _ensure_root_item()

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
        print(f"[CACHE] Erro cache upload: {e}")
        return None


# ==========================================
# WORKER DE BACKGROUND (Com Retry para Locks)
# ==========================================
def _worker_process_folder_cache(app, creds, folder_id, folder_path, include_files):
    subfolders_found = []
    items_count = 0
    max_retries = 3

    # Contexto ISOLADO
    with app.app_context():
        # Lógica de Retry para Database Locked
        for attempt in range(max_retries):
            try:
                # 1. Fetch remoto (Lento, mas não bloqueia banco)
                children = get_children(creds, folder_id, include_files=include_files)
                seen_at = datetime.utcnow()

                # 2. Prepara objetos
                items_to_add = []
                for child in children:
                    item = _upsert_item_from_remote(folder_id, folder_path, child, seen_at)
                    if item:
                        items_count += 1
                        if item.is_folder:
                            subfolders_found.append((item.drive_id, item.path))

                # 3. Commit (Ponto Crítico)
                db.session.commit()
                break  # Sucesso, sai do retry loop

            except OperationalError as e:
                db.session.rollback()
                if "locked" in str(e):
                    time.sleep(random.uniform(0.2, 1.0))  # Espera aleatória
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
    # Limpa sessão herdada
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

        # Começa pelo Root
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
                            t.files_downloaded = total_items  # Visual
                            t.message = f"Lendo: {path_processed[:40]}..."
                            db.session.commit()
                        except:
                            db.session.rollback()

                    for sid, spath in subs:
                        nf = executor.submit(_worker_process_folder_cache, app_obj, creds, sid, spath, include_files)
                        future_to_path[nf] = spath

                except Exception as e:
                    print(f"Erro path {path_processed}: {e}")

        # Finaliza
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