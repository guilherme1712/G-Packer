# app/models/audit_log.py
from .db_instance import db
from app.utils.time_utils import get_sp_now

class AuditLogModel(db.Model):
    __tablename__ = "audit_logs"

    id = db.Column(db.Integer, primary_key=True)
    user_email = db.Column(db.String(255), nullable=True)     # Quem (Email Google)
    action_type = db.Column(db.String(50), nullable=False)    # O que (DOWNLOAD, PROFILE_ADD, DELETE)
    target = db.Column(db.String(255), nullable=True)         # Alvo (Nome do arquivo, pasta, perfil)
    ip_address = db.Column(db.String(50), nullable=True)      # IP de Origem
    details = db.Column(db.Text, nullable=True)               # JSON/Texto (Tamanho, lista de arquivos)

    created_at = db.Column(db.DateTime(timezone=True), default=get_sp_now)

    def to_dict(self):
        return {
            "id": self.id,
            "user_email": self.user_email,
            "action_type": self.action_type,
            "target": self.target,
            "ip_address": self.ip_address,
            "details": self.details,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
