# models/backup_profile.py
from .db_instance import db
import time

class BackupProfile(db.Model):
    __tablename__ = 'backup_profiles'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    zip_pattern = db.Column(db.String(150), default="backup-{YYYYMMDD}")

    # Armazena a lista de itens e grupos como JSON
    items = db.Column(db.JSON, default=list)
    groups = db.Column(db.JSON, default=list)

    created_after = db.Column(db.String(20), nullable=True)
    modified_after = db.Column(db.String(20), nullable=True)

    max_size_mb = db.Column(db.Integer, nullable=True)

    archive_format = db.Column(db.String(20), default="zip")
    compression_level = db.Column(db.String(20), default="normal")
    output_mode = db.Column(db.String(20), default="archive")
    local_mirror_path = db.Column(db.String(500), nullable=True)

    def to_dict(self):
        """Converte o objeto do banco para um dicionário (usado nas APIs)."""
        return {
            "id": str(self.id), # Convertemos ID para string para manter compatibilidade com frontend
            "name": self.name,
            "zip_pattern": self.zip_pattern,
            "items": self.items or [],
            "groups": self.groups or [],
            "created_after": self.created_after,
            "modified_after": self.modified_after,
            "max_size_mb": self.max_size_mb,
            "archive_format": self.archive_format,
            "compression_level": self.compression_level,
            "output_mode": self.output_mode,
            "local_mirror_path": self.local_mirror_path,
        }
