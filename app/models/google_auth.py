from .db_instance import db
from app.utils.time_utils import get_sp_now, format_sp_time


class GoogleAuthModel(db.Model):
    __tablename__ = "google_auth"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=True)
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
        # to_dict padronizado retornando isoformat (ou use format_sp_time se preferir string amigável)
        # O GoogleAuthModel original usava isoformat(), mantive isso mas usando a data correta.
        return {
            "id": self.id,
            "email": self.email,
            "active": self.active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            # Se preferir visual: "created_at_fmt": format_sp_time(self.created_at)
        }