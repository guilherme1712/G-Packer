from .db_instance import db
from datetime import datetime
import json


class ScheduledTaskModel(db.Model):
    __tablename__ = 'scheduled_tasks'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    # Configuração do Backup
    items_json = db.Column(db.Text, nullable=False)  # JSON com IDs e Nomes do Drive
    zip_name = db.Column(db.String(150), default="backup_agendado")

    # Frequência: 'daily', 'weekly', 'monthly'
    frequency = db.Column(db.String(20), nullable=False)

    # Hora de execução (HH:MM)
    run_time = db.Column(db.String(5), default="02:00")

    # Controle de Execução
    last_run_at = db.Column(db.DateTime, nullable=True)
    # permitir nulo
    next_run_at = db.Column(db.DateTime(), nullable=True)

    # NOVO: vínculo opcional com perfil de backup
    profile_id = db.Column(db.Integer, db.ForeignKey('backup_profiles.id'), nullable=True)

    active = db.Column(db.Boolean, default=True)
    last_status = db.Column(db.String(500), nullable=True)  # Sucesso ou Erro

    # Relacionamento com BackupProfile (opcional)
    profile = db.relationship(
        "BackupProfile",
        backref=db.backref("scheduled_tasks", lazy="dynamic"),
        lazy="joined",
    )

    # Histórico de execuções
    runs = db.relationship(
        "ScheduledRunModel",
        backref="schedule",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "frequency": self.frequency,
            "run_time": self.run_time,
            "next_run_at": self.next_run_at.strftime("%d/%m/%Y %H:%M") if self.next_run_at else "-",
            "last_run_at": self.last_run_at.strftime("%d/%m/%Y %H:%M") if self.last_run_at else "-",
            "active": self.active,
            "last_status": self.last_status,
            "items_count": len(json.loads(self.items_json)) if self.items_json else 0,
            "profile_id": self.profile_id,
            "profile_name": self.profile.name if self.profile else None,
        }


class ScheduledRunModel(db.Model):
    """
    Histórico de execuções de cada agendamento.
    """
    __tablename__ = "scheduled_runs"

    id = db.Column(db.Integer, primary_key=True)
    schedule_id = db.Column(db.Integer, db.ForeignKey("scheduled_tasks.id"), nullable=False)

    started_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)

    status = db.Column(db.String(100), nullable=True)   # ex: "Sucesso", "Erro: xxx"
    size_mb = db.Column(db.Float, nullable=True)
    filename = db.Column(db.String(255), nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "started_at": self.started_at.strftime("%d/%m/%Y %H:%M") if self.started_at else "-",
            "finished_at": self.finished_at.strftime("%d/%m/%Y %H:%M") if self.finished_at else "-",
            "status": self.status,
            "size_mb": self.size_mb,
            "filename": self.filename,
        }
