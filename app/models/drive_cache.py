# app/models/drive_cache.py
from datetime import datetime

from .db_instance import db


class DriveItemCacheModel(db.Model):
    """
    Cache local de metadados de arquivos/pastas do Google Drive.

    Campos principais:
      - drive_id: ID original no Drive
      - name: nome do item
      - path: caminho "Meu Drive/..." calculado
      - mime_type: MIME do arquivo/pasta
      - is_folder: True se for pasta
      - parent_id: ID do pai no Drive
      - size_bytes: tamanho em bytes (0 para pastas)
      - modified_time: string ISO do Drive (última modificação)
      - trashed: se o item foi marcado como removido no cache
    """
    __tablename__ = "drive_cache"

    id = db.Column(db.Integer, primary_key=True)

    # ID no Drive
    drive_id = db.Column(db.String(128), unique=True, nullable=False, index=True)

    # Nome visível
    name = db.Column(db.String(1024), nullable=False, index=True)

    # Caminho completo calculado (Meu Drive/..., pode ser grande)
    path = db.Column(db.String(4096), index=True)

    # Metadados do tipo
    mime_type = db.Column(db.String(255), index=True)
    is_folder = db.Column(db.Boolean, default=False, index=True)

    # ID do pai no Drive
    parent_id = db.Column(db.String(128), index=True)

    # Tamanho (arquivos)
    size_bytes = db.Column(db.BigInteger, default=0, index=True)

    # String ISO vinda do Drive (ex: 2025-12-07T12:34:56.000Z)
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
        """
        Formato esperado pelo frontend da árvore (/api/folders/*).
        """
        return {
            "id": self.drive_id,
            "name": self.name,
            "type": "folder" if self.is_folder else "file",
            "size_bytes": self.size_bytes,
        }

    def to_dict(self) -> dict:
        """
        Formato mais completo para tela de busca ou debug.
        """
        return {
            "id": self.drive_id,
            "name": self.name,
            "path": self.path,
            "mime_type": self.mime_type,
            "is_folder": self.is_folder,
            "parent_id": self.parent_id,
            "size_bytes": self.size_bytes,
            "modified_time": self.modified_time,
            "trashed": self.trashed,
            "last_seen_remote": self.last_seen_remote.isoformat()
            if self.last_seen_remote
            else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"<DriveItemCache {self.drive_id} {self.path!r}>"
