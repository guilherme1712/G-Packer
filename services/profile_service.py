# services/profile_service.py
import json
import os
import time
from typing import Tuple, Optional

from config import PROFILES_FILE


def load_backup_profiles() -> list[dict]:
    """Carrega todos os perfis de backup do arquivo JSON."""
    if not os.path.exists(PROFILES_FILE):
        return []
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def save_backup_profiles(profiles: list[dict]) -> None:
    """Salva a lista completa de perfis no arquivo JSON."""
    try:
        with open(PROFILES_FILE, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
    except Exception:
        # Falha silenciosa para não quebrar o fluxo de download
        pass


def create_profile(data: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    Cria e persiste um novo perfil de backup a partir de um payload (JSON).
    Retorna (perfil, erro). Se erro != None, perfil será None.
    """
    name = (data.get("name") or "").strip()
    if not name:
        return None, "Nome do modelo é obrigatório."

    items = data.get("items") or []
    if not isinstance(items, list) or not items:
        return None, "Nenhum item selecionado para o modelo."

    zip_pattern = (data.get("zip_pattern") or "").strip()
    if not zip_pattern:
        zip_pattern = "backup-{YYYYMMDD}"

    groups = data.get("groups") or []
    if not isinstance(groups, list):
        groups = []

    created_after = data.get("created_after") or None
    modified_after = data.get("modified_after") or None

    max_size_mb = data.get("max_size_mb", None)
    try:
        if max_size_mb is not None:
            max_size_mb = int(max_size_mb)
    except (TypeError, ValueError):
        max_size_mb = None

    archive_format = (data.get("archive_format") or "zip").strip()
    compression_level = (data.get("compression_level") or "normal").strip()
    output_mode = (data.get("output_mode") or "archive").strip()
    local_mirror_path = (data.get("local_mirror_path") or "").strip()

    profiles = load_backup_profiles()
    profile_id = str(int(time.time() * 1000))

    profile = {
        "id": profile_id,
        "name": name,
        "zip_pattern": zip_pattern,
        "items": items,
        "groups": groups,
        "created_after": created_after,
        "modified_after": modified_after,
        "max_size_mb": max_size_mb,
        "archive_format": archive_format,
        "compression_level": compression_level,
        "output_mode": output_mode,
        "local_mirror_path": local_mirror_path,
    }

    profiles.append(profile)
    save_backup_profiles(profiles)
    return profile, None


def get_profile(profile_id: str) -> Optional[dict]:
    """Retorna um perfil específico pelo ID ou None se não existir."""
    profiles = load_backup_profiles()
    return next((p for p in profiles if p.get("id") == profile_id), None)


def delete_profile(profile_id: str) -> bool:
    """Remove um perfil. Retorna True se algo foi removido."""
    profiles = load_backup_profiles()
    new_profiles = [p for p in profiles if p.get("id") != profile_id]
    if len(new_profiles) == len(profiles):
        return False
    save_backup_profiles(new_profiles)
    return True
