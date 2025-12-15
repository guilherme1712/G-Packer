# app/models/google_auth.py
from .db_instance import db
from app.utils.time_utils import get_sp_now, format_sp_time


class GoogleAuthModel(db.Model):
    __tablename__ = "google_auth"

    id = db.Column(db.Integer, primary_key=True)

    # Informações do Perfil Google
    email = db.Column(db.String(255), nullable=True)
    name = db.Column(db.String(255), nullable=True)  # [NOVO] Nome do usuário
    picture = db.Column(db.String(1024), nullable=True)  # [NOVO] URL da foto (pode ser longa)

    token_json = db.Column(db.Text, nullable=False)
    active = db.Column(db.Boolean, default=True, nullable=False)

    # Usa o utilitário central
    created_at = db.Column(
        db.DateTime(timezone=True),
        default=get_sp_now,
        nullable=False
    )

    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=get_sp_now,
        onupdate=get_sp_now,
        nullable=False,
    )

    def to_dict(self):
        return {
            "id": self.id,
            "email": self.email,
            "name": self.name,  # [NOVO]
            "picture": self.picture,  # [NOVO]
            "active": self.active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }