# services/profile_service.py
from typing import Tuple, Optional
from app.models import db, BackupProfileModel

def load_backup_profiles() -> list[dict]:
    """
    Carrega todos os perfis de backup a partir da tabela backup_profiles.
    Retorna lista de dicionários já prontos para o frontend.
    """
    profiles = BackupProfileModel.query.order_by(BackupProfileModel.id.desc()).all()
    return [p.to_dict() for p in profiles]


def _parse_int_or_none(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def create_profile(data: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    Cria e persiste um novo perfil de backup no BANCO.
    Retorna (perfil_dict, erro). Se erro != None, perfil será None.
    """
    name = (data.get("name") or "").strip()
    if not name:
        return None, "Nome do modelo é obrigatório."

    items = data.get("items") or []
    if not isinstance(items, list) or not items:
        return None, "Nenhum item selecionado para o modelo."

    zip_name = (data.get("zip_name") or "").strip() or None
    zip_pattern = (data.get("zip_pattern") or "").strip() or "backup-{YYYYMMDD}"

    groups = data.get("groups") or []
    if not isinstance(groups, list):
        groups = []

    created_after = data.get("created_after") or None
    modified_after = data.get("modified_after") or None

    max_size_mb = _parse_int_or_none(data.get("max_size_mb"))

    archive_format = (data.get("archive_format") or "zip").strip()
    compression_level = (data.get("compression_level") or "normal").strip()
    output_mode = (data.get("output_mode") or "archive").strip()
    local_mirror_path = (data.get("local_mirror_path") or "").strip() or None

    execution_mode = (data.get("execution_mode") or "immediate").strip()
    
    # Define o padrão como sequential se não vier nada
    processing_mode = (data.get("processing_mode") or "sequential").strip()

    try:
        profile = BackupProfileModel(
            name=name,
            zip_pattern=zip_pattern,
            zip_name=zip_name,
            items=items,
            groups=groups,
            created_after=created_after,
            modified_after=modified_after,
            max_size_mb=max_size_mb,
            archive_format=archive_format,
            compression_level=compression_level,
            output_mode=output_mode,
            local_mirror_path=local_mirror_path,
            execution_mode=execution_mode,
            processing_mode=processing_mode,
        )
        db.session.add(profile)
        db.session.commit()
        return profile.to_dict(), None
    except Exception as e:
        db.session.rollback()
        return None, f"Erro ao salvar perfil de backup: {e}"


def get_profile(profile_id: str) -> Optional[dict]:
    try:
        pid = int(profile_id)
    except (TypeError, ValueError):
        return None

    profile = BackupProfileModel.query.get(pid)
    if not profile:
        return None
    return profile.to_dict()


def delete_profile(profile_id: str) -> bool:
    try:
        pid = int(profile_id)
    except (TypeError, ValueError):
        return False

    profile = BackupProfileModel.query.get(pid)
    if not profile:
        return False

    try:
        db.session.delete(profile)
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        return False