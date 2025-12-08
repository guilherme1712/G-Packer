# models/task.py
from datetime import timedelta

from .db_instance import db
from app.utils.time_utils import get_sp_now, format_sp_time


class TaskModel(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.String(50), primary_key=True)

    # Fase geral do processo (mapeando, baixando, compactando, concluido, erro, etc.)
    phase = db.Column(db.String(50))
    message = db.Column(db.String(255))

    # Estados de Controle
    canceled = db.Column(db.Boolean, default=False)
    paused = db.Column(db.Boolean, default=False)

    # Contadores principais
    files_found = db.Column(db.Integer, default=0)
    files_total = db.Column(db.Integer, default=0)
    files_downloaded = db.Column(db.Integer, default=0)

    # Tamanho total encontrado/baixado
    bytes_found = db.Column(db.BigInteger, default=0)
    bytes_downloaded = db.Column(db.BigInteger, default=0)

    # Erros
    errors_count = db.Column(db.Integer, default=0)

    # Métricas adicionais (podem ser calculadas on-the-fly; esses campos ajudam a guardar snapshots)
    avg_speed_mb_s = db.Column(db.Float, default=0.0)       # média de download em MB/s
    peak_speed_mb_s = db.Column(db.Float, default=0.0)      # pico registrado em MB/s
    compression_ratio = db.Column(db.Float, default=0.0)    # (tamanho_original / tamanho_compactado)

    last_error_at = db.Column(db.DateTime(timezone=True), nullable=True)
    last_error_msg = db.Column(db.String(255), nullable=True)

    # Histórico de logs do progresso (lista de dicts)
    history = db.Column(db.JSON, default=list)

    # Datas com Timezone SP
    created_at = db.Column(db.DateTime(timezone=True), default=get_sp_now)
    updated_at = db.Column(db.DateTime(timezone=True), default=get_sp_now, onupdate=get_sp_now)

    # ---------------------------------------------------
    # Helpers internos
    # ---------------------------------------------------
    def _calc_speed_mb_s(self) -> float:
        """
        Calcula velocidade média de download com base em bytes_downloaded
        e diferença created_at / updated_at.
        """
        try:
            if not self.bytes_downloaded or not self.created_at or not self.updated_at:
                return 0.0

            delta: timedelta = self.updated_at - self.created_at
            seconds = max(delta.total_seconds(), 1.0)
            mb = self.bytes_downloaded / (1024 * 1024)
            return round(mb / seconds, 2)
        except Exception:
            return 0.0

    def _safe_percent(self) -> float:
        """
        Calcula porcentagem do processo com base em files_total e files_downloaded.
        """
        if self.files_total and self.files_total > 0:
            return round((self.files_downloaded / self.files_total) * 100, 1)
        if self.phase == "concluido":
            return 100.0
        return 0.0

    # ---------------------------------------------------
    # Serialização
    # ---------------------------------------------------
    def to_dict(self):
        # Percentual concluído
        percent = self._safe_percent()

        # Velocidade média: se já houver um valor persistido em avg_speed_mb_s,
        # usamos ele; senão, calculamos dinamicamente.
        speed_mb_s = self.avg_speed_mb_s or self._calc_speed_mb_s()

        return {
            "id": self.id,
            "phase": self.phase,
            "message": self.message,
            "files_found": self.files_found,
            "files_total": self.files_total,
            "files_downloaded": self.files_downloaded,
            "bytes_found": self.bytes_found,
            "bytes_downloaded": self.bytes_downloaded,
            "errors": self.errors_count,
            "history": self.history or [],
            "canceled": self.canceled,
            "paused": self.paused,
            "percent": percent,
            # Métricas de performance
            "speed_mb_s": round(speed_mb_s, 2),
            "peak_speed_mb_s": round(self.peak_speed_mb_s or 0.0, 2),
            "compression_ratio": round(self.compression_ratio or 0.0, 2),
            # Erros
            "last_error_at": (
                format_sp_time(self.last_error_at, "%d/%m/%Y %H:%M:%S")
                if self.last_error_at
                else None
            ),
            "last_error_msg": self.last_error_msg,
            # Datas formatadas
            "created_at": format_sp_time(self.created_at, "%d/%m/%Y %H:%M:%S"),
            "updated_at": format_sp_time(self.updated_at, "%d/%m/%Y %H:%M:%S"),
        }
