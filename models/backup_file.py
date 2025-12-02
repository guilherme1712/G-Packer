# models/backup_file.py
from datetime import datetime
from .db_instance import db


class BackupFileModel(db.Model):
    __tablename__ = "backup_files"

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), unique=True, nullable=False)
    path = db.Column(db.String(512), nullable=False)
    size_mb = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # campos extras
    items_count = db.Column(db.Integer, default=0)
    origin_task_id = db.Column(db.String(50), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "filename": self.filename,
            "path": self.path,
            "size_mb": round(self.size_mb or 0.0, 2),
            "created_at": self.created_at.strftime("%d/%m/%Y %H:%M:%S")
            if self.created_at
            else "",
            "items_count": self.items_count or 0,
            "origin_task_id": self.origin_task_id,
        }
