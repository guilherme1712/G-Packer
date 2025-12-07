# models/backup_file.py
import os
from datetime import datetime, timedelta

from .db_instance import db


class BackupFileModel(db.Model):
    __tablename__ = "backup_files"

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    path = db.Column(db.String(1024), nullable=False)
    size_mb = db.Column(db.Float, nullable=True)
    items_count = db.Column(db.Integer, nullable=True)
    origin_task_id = db.Column(db.String(50), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "filename": self.filename,
            "path": self.path,
            "size_mb": self.size_mb,
            "items_count": self.items_count,
            "origin_task_id": self.origin_task_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


def apply_global_retention(
    max_backups: int | None = None,
    max_days: int | None = None,
) -> None:
    """
    Aplica uma política de retenção global na tabela backup_files.

      - max_backups > 0 => mantém apenas os N backups mais recentes
      - max_days   > 0 => remove backups mais antigos que N dias

    Se ambos forem informados, a remoção é a união das duas regras
    (idade E/OU quantidade).
    """

    # Nada configurado? Não faz nada.
    if not max_backups and not max_days:
        return

    try:
        # Ordena do mais novo para o mais velho
        backups = (
            BackupFileModel.query
            .order_by(BackupFileModel.created_at.desc())
            .all()
        )

        if not backups:
            return

        # Conjunto de registros que serão apagados
        to_delete: set[BackupFileModel] = set()

        # Regra por quantidade
        if max_backups and max_backups > 0 and len(backups) > max_backups:
            excedentes = backups[max_backups:]  # joga fora do N-ésimo em diante
            for b in excedentes:
                to_delete.add(b)

        # Regra por idade (dias)
        if max_days and max_days > 0:
            cutoff = datetime.utcnow() - timedelta(days=max_days)
            for b in backups:
                if b.created_at and b.created_at < cutoff:
                    to_delete.add(b)

        if not to_delete:
            return

        # Remove arquivos físicos e apaga registros
        for b in to_delete:
            if b.path and os.path.exists(b.path):
                try:
                    os.remove(b.path)
                except Exception as e:
                    # Log simples em stdout; não queremos quebrar por causa disso
                    print(f"Erro ao remover arquivo de backup '{b.path}': {e}")

            try:
                db.session.delete(b)
            except Exception as e:
                print(f"Erro ao marcar backup '{b.filename}' para remoção: {e}")

        try:
            db.session.commit()
        except Exception as e:
            print(f"Erro ao aplicar commit da retenção de backups: {e}")
            db.session.rollback()

    except Exception as outer_err:
        # Nunca deixamos a retenção matar o processo de backup
        print(f"Erro inesperado ao aplicar retenção de backups: {outer_err}")
