from .db_instance import db
from datetime import datetime

class TaskModel(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.String(50), primary_key=True)
    phase = db.Column(db.String(50))
    message = db.Column(db.String(255))

    # Contadores
    files_found = db.Column(db.Integer, default=0)
    files_total = db.Column(db.Integer, default=0)
    files_downloaded = db.Column(db.Integer, default=0)
    bytes_found = db.Column(db.BigInteger, default=0)
    errors_count = db.Column(db.Integer, default=0)

    # Histórico de logs (Salvo como JSON)
    history = db.Column(db.JSON, default=list)

    canceled = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "phase": self.phase,
            "message": self.message,
            "files_found": self.files_found,
            "files_total": self.files_total,
            "files_downloaded": self.files_downloaded,
            "bytes_found": self.bytes_found,
            "errors": self.errors_count,
            "history": self.history or [],
            "canceled": self.canceled,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
