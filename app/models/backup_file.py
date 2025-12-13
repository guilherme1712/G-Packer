# models/backup_file.py
import os
import stat
import hashlib
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Set

from .db_instance import db


class BackupFileModel(db.Model):
    """
    Representa um arquivo de backup físico (ZIP/TAR/etc), agora
    com suporte a versionamento/snapshots em séries.

    Cada série de backup (series_key) forma uma linha do tempo
    de snapshots, ordenados por version_index.

    IMPORTANTE:
    - Cada linha desta tabela é sempre UM arquivo físico no disco
      (full ou incremental).
    - O campo series_key agrupa backups relacionados (por perfil,
      agendamento, tarefa, etc).
    """

    __tablename__ = "backup_files"

    id = db.Column(db.Integer, primary_key=True)

    # Arquivo físico
    filename = db.Column(db.String(255), nullable=False)
    # No projeto atual, este campo costuma armazenar o CAMINHO COMPLETO
    # do arquivo de backup (incluindo o nome).
    path = db.Column(db.String(1024), nullable=False)

    size_mb = db.Column(db.Float, nullable=True)
    items_count = db.Column(db.Integer, nullable=True)
    encrypted = db.Column(db.Boolean, default=False)

    # Metadados gerais
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    # Ex: id da TaskModel, do agendamento, etc.
    origin_task_id = db.Column(db.String(64), nullable=True)

    # Cache da estrutura interna (para explorador de conteúdo).
    # Ex: {"tree": [...], "total_size_bytes": 123, ...}
    structure_cache = db.Column(db.JSON, nullable=True)

    # -------------------------------
    # CAMPOS DE VERSIONAMENTO/SNAPSHOT
    # -------------------------------

    # Série lógica de backups (ex: "sched:5", "manual:perfil_3", etc.)
    series_key = db.Column(db.String(255), index=True, nullable=True)

    # Versão incremental dentro da série (1, 2, 3, ...)
    version_index = db.Column(db.Integer, nullable=True, index=True)

    # True = backup full; False = incremental (parent_id aponta pro pai)
    is_full = db.Column(db.Boolean, default=True)

    # Snapshot pai (somente para incrementais)
    parent_id = db.Column(db.Integer, db.ForeignKey("backup_files.id"), nullable=True)
    parent = db.relationship(
        "BackupFileModel",
        remote_side=[id],
        backref=db.backref("children", lazy="dynamic"),
    )

    # Metadados extras da execução (manifest, filtros, etc.)
    metadata_json = db.Column(db.JSON, nullable=True)

    # -------------------------------
    # HELPERS BÁSICOS
    # -------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "filename": self.filename,
            "path": self.path,
            "size_mb": self.size_mb,
            "items_count": self.items_count,
            "encrypted": self.encrypted,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "origin_task_id": self.origin_task_id,
            "series_key": self.series_key,
            "version_index": self.version_index,
            "is_full": self.is_full,
            "parent_id": self.parent_id,
            "metadata": self.metadata_json or {},
        }

    @property
    def full_path(self) -> str:
        """
        Caminho completo no disco.

        No seu projeto, o campo `path` já costuma guardar o caminho
        completo, incluindo o nome do arquivo. Este helper tenta ser
        o mais tolerante possível para manter compatibilidade.
        """
        if self.path and os.path.isabs(self.path):
            # já é um caminho absoluto, provavelmente completo
            return self.path
        # fallback: junta path + filename
        return os.path.join(self.path or "", self.filename or "")

    @staticmethod
    def build_series_key(zip_base_name: Optional[str], items: Optional[list[dict]]) -> str:
        """
        Gera uma chave estável de série com base:
        - no nome base do ZIP (sem extensão)
        - + hash da lista de IDs dos itens do Drive

        Assim:
        - Se o usuário gerar vários backups com os MESMOS itens,
          todos caem na mesma série (v1, v2, v3...).
        - Se mudar muito a seleção de itens, a série muda.
        """
        base = (zip_base_name or "").strip() or "backup"

        # Remove extensões comuns
        lower = base.lower()
        if lower.endswith(".tar.gz"):
            base = base[:-7]
        elif "." in base:
            base = base.rsplit(".", 1)[0]

        # Nome legível da série
        clean = re.sub(r"[^a-zA-Z0-9_-]+", "-", base)
        clean = clean.strip("-") or "backup"

        # Coleta IDs dos itens (pastas/arquivos)
        ids = []
        if items:
            for it in items:
                if not isinstance(it, dict):
                    continue
                vid = it.get("id") or it.get("file_id")
                if vid:
                    ids.append(str(vid))

        if ids:
            ids = sorted(set(ids))
            key_src = "|".join(ids)
            digest = hashlib.sha1(key_src.encode("utf-8")).hexdigest()[:10]
            return f"{clean}:{digest}"

        # fallback: só pelo nome
        return f"{clean}:manual"

    # -------------------------------
    # SNAPSHOTS / SÉRIES
    # -------------------------------

    @staticmethod
    def _normalize_series_key(series_key, origin_task_id=None):
        if series_key: return series_key
        return f"legacy_{origin_task_id}" if origin_task_id else "unknown"

    @classmethod
    def get_last_snapshot(cls, series_key: str) -> Optional["BackupFileModel"]:
        """
        Retorna o último snapshot (maior version_index) de uma série.
        """
        if not series_key:
            return None
        return (
            cls.query.filter_by(series_key=series_key)
            .order_by(cls.version_index.desc())
            .first()
        )

    @classmethod
    def get_series_snapshots(cls, series_key: str) -> List["BackupFileModel"]:
        """
        Lista todos os snapshots de uma série, ordenados por versão.
        """
        if not series_key:
            return []
        return (
            cls.query.filter_by(series_key=series_key)
            .order_by(cls.version_index.asc())
            .all()
        )

    @classmethod
    def register_snapshot(
        cls,
        *,
        filename: str,
        path: str,
        size_mb: Optional[float],
        items_count: Optional[int],
        origin_task_id: Optional[str] = None,
        series_key: Optional[str] = None,
        is_full: Optional[bool] = None,
        parent_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "BackupFileModel":
        """
        Registra um novo snapshot na tabela, calculando automaticamente
        o próximo version_index da série.

        Comportamento:
        - Se is_full for None: o primeiro snapshot da série vira FULL;
          os demais são INCREMENTAIS e apontam para o último snapshot.
        - Se is_full=True: parent_id é ignorado.
        - Se is_full=False: parent_id DEVE apontar para algum snapshot anterior.
        """
        series = cls._normalize_series_key(series_key, origin_task_id)
        last = cls.get_last_snapshot(series)
        next_version = (last.version_index if last and last.version_index else 0) + 1

        if is_full is None:
            # auto: primeiro é full, os demais são incrementais
            is_full = last is None

        if is_full:
            parent_id = None
        else:
            # se não for full e não tiver parent_id, assume último snapshot
            if parent_id is None and last is not None:
                parent_id = last.id

        safe_metadata = cls._normalize_for_json(metadata or {})

        obj = cls(
            filename=filename,
            path=path,
            size_mb=size_mb,
            items_count=items_count,
            origin_task_id=origin_task_id,
            series_key=series,
            version_index=next_version,
            is_full=is_full,
            parent_id=parent_id,
            metadata_json=safe_metadata,
        )
    
        db.session.add(obj)
        db.session.commit()
        return obj

    @classmethod
    def list_all_series(cls) -> List[str]:
        """
        Retorna todas as series_key distintas existentes.
        Útil para telas de administração/agrupamento.
        """
        rows = (
            db.session.query(cls.series_key)
            .filter(cls.series_key.isnot(None))
            .distinct()
            .all()
        )
        return [r[0] for r in rows]

    # -------------------------------
    # RETENÇÃO (POR SÉRIE)
    # -------------------------------
    @classmethod
    def apply_retention(
        cls,
        max_backups: Optional[int],
        max_days: Optional[int],
        storage_root_path: Optional[str] = None,
    ) -> None:
        """
        Aplica política de retenção global.

        - max_backups: máximo de snapshots por série (se None/0, ignora).
        - max_days: apaga snapshots mais antigos que X dias (se None/0, ignora).
        - storage_root_path: caminho base do diretório de backups (opcional).
        """
        try:
            backups: List[BackupFileModel] = (
                cls.query.order_by(cls.created_at.desc()).all()
            )
            if not backups:
                return

            to_delete: Set[BackupFileModel] = set()

            # Agrupamento por série
            series_map: Dict[str, List[BackupFileModel]] = {}
            for b in backups:
                skey = b.series_key or "manual:default"
                series_map.setdefault(skey, []).append(b)

            # Regra por quantidade -> por série
            if max_backups and max_backups > 0:
                for skey, series_backups in series_map.items():
                    # ordena desc (mais novo primeiro)
                    series_backups_sorted = sorted(
                        series_backups,
                        key=lambda x: x.created_at or datetime.min,
                        reverse=True,
                    )
                    excedentes = series_backups_sorted[max_backups:]
                    for b in excedentes:
                        to_delete.add(b)

            # Regra por idade (dias) -> global
            if max_days and max_days > 0:
                cutoff = datetime.utcnow() - timedelta(days=max_days)
                for b in backups:
                    if b.created_at and b.created_at < cutoff:
                        to_delete.add(b)

            if not to_delete:
                return

            # Remoção física + delete na base
            for b in to_delete:
                try:
                    cls._delete_file_and_row(b, storage_root_path)
                except Exception as e:
                    print(f"[Retention] Erro ao remover backup '{b.filename}': {e}")

            try:
                db.session.commit()
            except Exception as e:
                print(
                    f"[Retention] Erro ao aplicar commit da retenção de backups: {e}"
                )
                db.session.rollback()

        except Exception as outer_err:
            # Nunca deixamos a retenção quebrar o processo principal de backup
            print(f"[Retention] Erro inesperado ao aplicar retenção de backups: {outer_err}")

    @classmethod
    def _delete_file_and_row(
        cls,
        backup: "BackupFileModel",
        storage_root_path: Optional[str] = None,
    ) -> None:
        """
        Exclui o arquivo físico (se existir) e a linha na tabela.
        """
        full_path = backup.full_path

        # Se foi passado um storage_root_path diferente, tenta ajustar.
        # No seu uso atual, normalmente isso vem como None.
        if storage_root_path and os.path.isabs(storage_root_path):
            try:
                rel = os.path.relpath(full_path, start=storage_root_path)
                full_path = os.path.join(storage_root_path, rel)
            except Exception:
                # Se der erro na normalização, segue com o caminho original.
                pass

        try:
            if os.path.exists(full_path):
                # remove flag de "somente leitura" se houver
                file_stat = os.stat(full_path)
                if not (file_stat.st_mode & stat.S_IWRITE):
                    os.chmod(full_path, stat.S_IWRITE)
                os.remove(full_path)
        except Exception as e:
            print(f"[Retention] Erro ao apagar arquivo físico '{full_path}': {e}")

        # remove da base
        db.session.delete(backup)
        
    # -------------------------------
    # JSON SAFE HELPER
    # -------------------------------
    @staticmethod
    def _normalize_for_json(value):
        """
        Converte estruturas que o JSON não entende (set, datetime, etc.)
        para tipos compatíveis (list, str, ...). Funciona de forma recursiva.
        """
        from datetime import datetime, date

        # Sets / frozensets -> list
        if isinstance(value, (set, frozenset)):
            return [BackupFileModel._normalize_for_json(v) for v in value]

        # Tuplas -> list (JSON não diferencia)
        if isinstance(value, tuple):
            return [BackupFileModel._normalize_for_json(v) for v in value]

        # Dict -> dict normalizando recursivamente
        if isinstance(value, dict):
            return {
                str(k): BackupFileModel._normalize_for_json(v)
                for k, v in value.items()
            }

        # List -> normaliza cada item
        if isinstance(value, list):
            return [BackupFileModel._normalize_for_json(v) for v in value]

        # datetime / date -> ISO string
        if isinstance(value, (datetime, date)):
            return value.isoformat()

        # Qualquer outro tipo diferente mas simples pode ser deixado como está
        # (int, float, str, bool, None, etc.)
        return value

# -----------------------------------------------------------------------------
# Wrapper de conveniência p/ o controller do Drive
# -----------------------------------------------------------------------------
def apply_global_retention(
    max_backups: Optional[int],
    max_days: Optional[int],
    storage_root_path: Optional[str] = None,
) -> None:
    """
    Função simples usada pelos controllers para aplicar a política de retenção
    sem precisar importar diretamente os detalhes da classe.
    """
    BackupFileModel.apply_retention(
        max_backups=max_backups,
        max_days=max_days,
        storage_root_path=storage_root_path,
    )
