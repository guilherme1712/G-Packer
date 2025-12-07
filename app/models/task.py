from .db_instance import db
from app.utils.time_utils import get_sp_now, format_sp_time

class TaskModel(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.String(50), primary_key=True)
    phase = db.Column(db.String(50))
    message = db.Column(db.String(255))

    # Estados de Controle
    canceled = db.Column(db.Boolean, default=False)
    paused = db.Column(db.Boolean, default=False)

    # Contadores
    files_found = db.Column(db.Integer, default=0)
    files_total = db.Column(db.Integer, default=0)
    files_downloaded = db.Column(db.Integer, default=0)
    bytes_found = db.Column(db.BigInteger, default=0)
    errors_count = db.Column(db.Integer, default=0)

    # Histórico de logs
    history = db.Column(db.JSON, default=list)

    # Datas com Timezone SP
    created_at = db.Column(db.DateTime(timezone=True), default=get_sp_now)
    updated_at = db.Column(db.DateTime(timezone=True), default=get_sp_now, onupdate=get_sp_now)

    def to_dict(self):
        # Calcula porcentagem segura
        percent = 0
        if self.files_total and self.files_total > 0:
            percent = (self.files_downloaded / self.files_total) * 100
        elif self.phase == 'concluido':
            percent = 100

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
            "paused": self.paused,
            "percent": round(percent, 1),
            # Formata usando o utilitário
            "created_at": format_sp_time(self.created_at, "%d/%m/%Y %H:%M:%S"),
            "updated_at": format_sp_time(self.updated_at, "%d/%m/%Y %H:%M:%S"),
        }