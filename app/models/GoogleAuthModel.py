from datetime import datetime
from . import db  # ou de onde você já importa o db
import pytz

def get_sp_time():
    sp_tz = pytz.timezone('America/Sao_Paulo')
    return datetime.now(sp_tz)

class GoogleAuthModel(db.Model):
    __tablename__ = "google_auth"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=True)
    token_json = db.Column(db.Text, nullable=False)
    active = db.Column(db.Boolean, default=True, nullable=False)

    created_at = db.Column(
        db.DateTime(timezone=True), 
        default=get_sp_time, 
        nullable=False
    )
    
    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=get_sp_time,
        onupdate=get_sp_time,
        nullable=False,
    )

    def to_dict(self):
        return {
            "id": self.id,
            "email": self.email,
            "active": self.active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
