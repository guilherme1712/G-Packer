# models/models_scheduler.py
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
    # >>> IMPORTANTE: permitir nulo
    next_run_at = db.Column(db.DateTime, nullable=True)

    active = db.Column(db.Boolean, default=True)
    last_status = db.Column(db.String(500), nullable=True)  # Sucesso ou Erro

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
            "items_count": len(json.loads(self.items_json)) if self.items_json else 0
        }