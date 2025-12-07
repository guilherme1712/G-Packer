from .db_instance import db
import json
from app.utils.time_utils import get_sp_now, format_sp_time

class ScheduledRunModel(db.Model):
    __tablename__ = "scheduled_runs"

    id = db.Column(db.Integer, primary_key=True)
    schedule_id = db.Column(db.Integer, db.ForeignKey("scheduled_tasks.id"), nullable=False)

    # Datas com Timezone SP
    started_at = db.Column(db.DateTime(timezone=True), nullable=False, default=get_sp_now)
    finished_at = db.Column(db.DateTime(timezone=True), nullable=True)

    status = db.Column(db.String(100), nullable=True)
    size_mb = db.Column(db.Float, nullable=True)
    filename = db.Column(db.String(255), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "started_at": format_sp_time(self.started_at),
            "finished_at": format_sp_time(self.finished_at),
            "status": self.status,
            "size_mb": self.size_mb,
            "filename": self.filename,
        }