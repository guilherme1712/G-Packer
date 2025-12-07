# app/services/Google/drive_cache.py
from datetime import datetime, timedelta

from flask import current_app

from app.models.db_instance import db
from config import TIMEZONE
from app.models.drive_cache import DriveItemCacheModel
from .drive_tree import get_children

# TTL padrão do cache por pasta (5 minutos)
DEFAULT_MAX_AGE_SECONDS = 300


def _ensure_root_item() -> DriveItemCacheModel:
    """
    Garante que exista um registro "root" no cache.
    """
    root = DriveItemCacheModel.query.filter_by(drive_id="root").first()
    if root:
        return root

    root = DriveItemCacheModel(
        drive_id="root",
        name="Meu Drive",
        mime_type="application/vnd.google-apps.folder",
        is_folder=True,
        parent_id=None,
        path="Meu Drive",
        trashed=False,
    )
    db.session.add(root)
    db.session.commit()
    return root


def _upsert_item_from_remote(
    parent_id: str,
    parent_path: str,
    item_data: dict,
    seen_at: datetime,
) -> DriveItemCacheModel:
    """
    Converte um dict vindo de get_children() em uma linha de DriveItemCache.
    """
    drive_id = item_data["id"]
    name = item_data["name"]
    is_folder = item_data["type"] == "folder"
    mime_type = item_data.get("mimeType") or (
        "application/vnd.google-apps.folder" if is_folder else None
    )
    size_bytes = int(item_data.get("size_bytes") or 0)
    modified_time = item_data.get("modified_time")

    path = f"{parent_path}/{name}" if parent_path else name

    item = DriveItemCacheModel.query.filter_by(drive_id=drive_id).first()
    if not item:
        item = DriveItemCacheModel(drive_id=drive_id)

    item.name = name
    item.mime_type = mime_type
    item.is_folder = is_folder
    item.parent_id = parent_id
    item.path = path
    item.size_bytes = size_bytes
    item.modified_time = modified_time
    item.trashed = False
    item.last_seen_remote = seen_at

    db.session.add(item)
    return item


def _refresh_folder_from_remote(creds, folder_id: str, include_files: bool = True):
    """
    Faz uma chamada ao Drive para buscar filhos da pasta e atualiza o cache.
    Sempre retorna a lista de items conforme get_children().
    """
    _ensure_root_item()

    # Descobre o path do pai (se existir no cache)
    if folder_id == "root":
        parent_path = "Meu Drive"
    else:
        parent = DriveItemCacheModel.query.filter_by(drive_id=folder_id).first()
        parent_path = parent.path if parent and parent.path else ""

    children = get_children(creds, folder_id, include_files=True)
    seen_at = datetime.utcnow()

    # Remove registros antigos dos filhos desse pai para evitar lixo
    DriveItemCacheModel.query.filter_by(parent_id=folder_id).delete()

    for child in children:
        _upsert_item_from_remote(folder_id, parent_path, child, seen_at)

    db.session.commit()
    return children


def get_children_cached(
    creds,
    folder_id: str,
    include_files: bool = False,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    force_refresh: bool = False,
):
    """
    Versão com cache de get_children():

    - Se a pasta tiver filhos em cache “recentes”, devolve só do banco.
    - Se estiver vazia ou velho demais, chama o Drive, atualiza o cache e devolve.
    """
    _ensure_root_item()

    if force_refresh:
        children = _refresh_folder_from_remote(creds, folder_id, include_files=True)
    else:
        now = datetime.now()
        cached_children = (
            DriveItemCacheModel.query.filter_by(parent_id=folder_id, trashed=False)
            .order_by(DriveItemCacheModel.is_folder.desc(), DriveItemCacheModel.name.asc())
            .all()
        )

        if cached_children:
            latest_seen = max(
                c.last_seen_remote or c.updated_at or c.created_at
                for c in cached_children
            )
            age = (now - latest_seen).total_seconds()
            if age <= max_age_seconds:
                # Cache "fresco": devolve direto
                return [
                    c.to_tree_node()
                    for c in cached_children
                    if include_files or c.is_folder
                ]

        # Sem cache ou muito velho: busca remoto
        children = _refresh_folder_from_remote(creds, folder_id, include_files=True)

    # Normaliza a resposta no mesmo formato da árvore
    result = []
    for child in children:
        if child["type"] == "file" and not include_files:
            continue
        result.append(
            {
                "id": child["id"],
                "name": child["name"],
                "type": child["type"],
                "size_bytes": child.get("size_bytes"),
            }
        )
    return result


def rebuild_full_cache(creds, include_files: bool = True) -> int:
    """
    Limpa a tabela inteira e refaz o cache da árvore a partir de 'root'.

    CUIDADO: pode ser pesado em Drives muito grandes, por causa das chamadas
    recursivas.

    Retorna o número total de itens cacheados.
    """
    current_app.logger.info("Recriando cache completo do Drive...")

    # Apaga tudo
    DriveItemCacheModel.query.delete()
    db.session.commit()

    root = _ensure_root_item()
    total = 0

    # BFS simples usando get_children()
    queue = [("root", root.path or "Meu Drive")]

    while queue:
        folder_id, folder_path = queue.pop(0)
        children = get_children(creds, folder_id, include_files=True)
        seen_at = datetime.utcnow()

        # remove filhos antigos desse pai
        DriveItemCacheModel.query.filter_by(parent_id=folder_id).delete()

        for child in children:
            item = _upsert_item_from_remote(folder_id, folder_path, child, seen_at)
            total += 1
            if item.is_folder:
                queue.append((item.drive_id, item.path))

        db.session.commit()

    current_app.logger.info("Cache completo do Drive recriado: %s itens", total)
    return total


def search_cache(
    text: str | None = None,
    type_filter: str | None = None,  # "file", "folder" ou None
    min_size: int | None = None,
    max_size: int | None = None,
    limit: int = 200,
) -> list[dict]:
    """
    Busca em cima da tabela de cache sem chamar o Drive.

    Filtros:
      - text: trecho do nome (ILIKE)
      - type_filter: "file" ou "folder"
      - min_size / max_size: em bytes
    """
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
