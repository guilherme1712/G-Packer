from .db_instance import db
from .backup_profile import BackupProfileModel
import json
from app.utils.time_utils import get_sp_now, format_sp_time


class ScheduledTaskModel(db.Model):
    __tablename__ = 'scheduled_tasks'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    items_json = db.Column(db.Text, nullable=False)
    zip_name = db.Column(db.String(150), default="backup_agendado")
    frequency = db.Column(db.String(20), nullable=False)
    run_time = db.Column(db.String(5), default="02:00")

    # Datas com Timezone SP
    last_run_at = db.Column(db.DateTime(timezone=True), nullable=True)
    next_run_at = db.Column(db.DateTime(timezone=True), nullable=True)

    profile_id = db.Column(db.Integer, db.ForeignKey('backup_profiles.id'), nullable=True)
    active = db.Column(db.Boolean, default=True)
    last_status = db.Column(db.String(500), nullable=True)

    # CORREÇÃO AQUI:
    # Usando a classe BackupProfileModel diretamente em vez da string "BackupProfile" incorreta.
    profile = db.relationship(BackupProfileModel, backref=db.backref("scheduled_tasks", lazy="dynamic"), lazy="joined")

    runs = db.relationship("ScheduledRunModel", backref="schedule", lazy="dynamic", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "frequency": self.frequency,
            "run_time": self.run_time,
            "zip_name": self.zip_name,
            "next_run_at": format_sp_time(self.next_run_at),
            "last_run_at": format_sp_time(self.last_run_at),
            "active": self.active,
            "last_status": self.last_status,
            "items_count": len(json.loads(self.items_json)) if self.items_json else 0,
            "profile_id": self.profile_id,
            "profile_name": self.profile.name if self.profile else None,
        }