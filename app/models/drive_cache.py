# app/models/drive_cache.py
from datetime import datetime
from .db_instance import db

class DriveItemCacheModel(db.Model):
    """
    Cache local de metadados de arquivos/pastas do Google Drive.
    """
    __tablename__ = "drive_cache"

    id = db.Column(db.Integer, primary_key=True)

    # ID no Drive
    drive_id = db.Column(db.String(128), unique=True, nullable=False, index=True)

    # Nome visível
    name = db.Column(db.String(1024), nullable=False, index=False)

    # Caminho completo calculado
    path = db.Column(db.String(4096), index=False)

    # Metadados do tipo
    mime_type = db.Column(db.String(255), index=True)
    is_folder = db.Column(db.Boolean, default=False, index=True)

    # ID do pai no Drive
    parent_id = db.Column(db.String(128), index=True)

    # Tamanho (arquivos)
    size_bytes = db.Column(db.BigInteger, default=0, index=True)
    
    # NOVO: Checksum MD5 para deduplicação (vem da API do Drive)
    md5_checksum = db.Column(db.String(32), index=True, nullable=True)

    # String ISO vinda do Drive
    modified_time = db.Column(db.String(64), index=True)

    # Se esse item foi “descartado” no cache
    trashed = db.Column(db.Boolean, default=False, index=True)

    # Controle de atualização
    last_seen_remote = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    def to_tree_node(self) -> dict:
        return {
            "id": self.drive_id,
            "name": self.name,
            "type": "folder" if self.is_folder else "file",
            "size_bytes": self.size_bytes,
        }

    def to_dict(self) -> dict:
        return {
            "id": self.drive_id,
            "name": self.name,
            "path": self.path,
            "mime_type": self.mime_type,
            "is_folder": self.is_folder,
            "parent_id": self.parent_id,
            "size_bytes": self.size_bytes,
            "md5_checksum": self.md5_checksum, # Incluído no retorno
            "modified_time": self.modified_time,
            "trashed": self.trashed,
            "last_seen_remote": self.last_seen_remote.isoformat() if self.last_seen_remote else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return f"<DriveItemCache {self.drive_id} {self.path!r}>"