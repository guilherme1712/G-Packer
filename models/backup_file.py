from .db_instance import db
from datetime import datetime

class BackupFileModel(db.Model):
    __tablename__ = 'backup_files'

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), unique=True, nullable=False)
    path = db.Column(db.String(512), nullable=False)
    size_mb = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.now)

    # Campos opcionais para filtros futuros
    items_count = db.Column(db.Integer, default=0)
    origin_task_id = db.Column(db.String(50), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "filename": self.filename,
            "path": self.path,
            "size_mb": self.size_mb,
            "created_at": self.created_at.strftime('%d/%m/%Y %H:%M:%S'),
            "items_count": self.items_count
        }
