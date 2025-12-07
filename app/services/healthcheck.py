import os
import shutil
import time
from typing import Dict, Any

from flask import current_app
from sqlalchemy import text

from app.models import db  # ajuste o caminho se for diferente
from app.utils.structured_logging import log_event

# Google Auth
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
except ImportError:  # se a lib não estiver instalada
    Credentials = None
    Request = None


def _status_level_from_free_ratio(free_ratio: float) -> str:
    """
    Converte % livre em status:
    > 20%  -> ok
    10-20% -> warning
    < 10%  -> error
    """
    if free_ratio <= 0.10:
        return "error"
    if free_ratio <= 0.20:
        return "warning"
    return "ok"


def check_database() -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        # simple health query
        db.session.execute(text("SELECT 1"))
        db.session.commit()
        duration_ms = (time.perf_counter() - started) * 1000
        status = "ok"
        message = "Conexão com banco OK."
        log_event(
            "health.database",
            "INFO",
            status=status,
            duration_ms=round(duration_ms, 2),
        )
        return {
            "name": "Banco de Dados",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }
    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.perf_counter() - started) * 1000
        status = "error"
        message = f"Erro ao conectar no banco: {exc!r}"
        log_event(
            "health.database",
            "ERROR",
            status=status,
            duration_ms=round(duration_ms, 2),
            error=str(exc),
        )
        return {
            "name": "Banco de Dados",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }


def check_google_credentials() -> Dict[str, Any]:
    started = time.perf_counter()

    if Credentials is None or Request is None:
        duration_ms = (time.perf_counter() - started) * 1000
        status = "warning"
        message = "Bibliotecas google-auth não instaladas; não foi possível validar o token."
        log_event(
            "health.google",
            "WARNING",
            status=status,
            duration_ms=round(duration_ms, 2),
        )
        return {
            "name": "Credenciais Google",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }

    token_path = current_app.config.get("GOOGLE_TOKEN_FILE", "token.json")

    if not os.path.exists(token_path):
        duration_ms = (time.perf_counter() - started) * 1000
        status = "error"
        message = f"Arquivo de token Google não encontrado: {token_path}"
        log_event(
            "health.google",
            "ERROR",
            status=status,
            duration_ms=round(duration_ms, 2),
            token_path=token_path,
        )
        return {
            "name": "Credenciais Google",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }

    try:
        creds = Credentials.from_authorized_user_file(token_path)
        # credenciais OK e não expiram logo
        if creds.valid:
            duration_ms = (time.perf_counter() - started) * 1000
            status = "ok"
            message = "Token Google válido."
        else:
            # Tenta refresh se possível
            if creds.expired and creds.refresh_token:
                request = Request()
                creds.refresh(request)
                duration_ms = (time.perf_counter() - started) * 1000
                if creds.valid:
                    status = "warning"
                    message = "Token Google estava expirado, mas foi renovado com sucesso."
                else:
                    status = "error"
                    message = "Token Google inválido mesmo após tentar renovar."
            else:
                duration_ms = (time.perf_counter() - started) * 1000
                status = "error"
                message = "Token Google inválido ou expirado e sem refresh_token."

        log_event(
            "health.google",
            "INFO" if status == "ok" else "WARNING" if status == "warning" else "ERROR",
            status=status,
            duration_ms=round(duration_ms, 2),
        )

        return {
            "name": "Credenciais Google",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }

    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.perf_counter() - started) * 1000
        status = "error"
        message = f"Erro ao validar credenciais Google: {exc!r}"
        log_event(
            "health.google",
            "ERROR",
            status=status,
            duration_ms=round(duration_ms, 2),
            error=str(exc),
        )
        return {
            "name": "Credenciais Google",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }


def check_backup_disk() -> Dict[str, Any]:
    started = time.perf_counter()
    backup_root = current_app.config.get("BACKUP_ROOT")

    if not backup_root:
        duration_ms = (time.perf_counter() - started) * 1000
        status = "error"
        message = "Config BACKUP_ROOT não definida."
        log_event(
            "health.backup_disk",
            "ERROR",
            status=status,
            duration_ms=round(duration_ms, 2),
        )
        return {
            "name": "Espaço em disco (backups)",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
        }

    if not os.path.exists(backup_root):
        duration_ms = (time.perf_counter() - started) * 1000
        status = "warning"
        message = f"Pasta de backups ainda não existe: {backup_root}"
        log_event(
            "health.backup_disk",
            "WARNING",
            status=status,
            duration_ms=round(duration_ms, 2),
            path=backup_root,
        )
        return {
            "name": "Espaço em disco (backups)",
            "status": status,
            "message": message,
            "duration_ms": round(duration_ms, 2),
            "details": {
                "path": backup_root,
            },
        }

    usage = shutil.disk_usage(backup_root)
    total = usage.total
    free = usage.free
    used = usage.used
    free_ratio = free / total if total else 0

    status = _status_level_from_free_ratio(free_ratio)

    if status == "ok":
        message = "Espaço em disco adequado."
    elif status == "warning":
        message = "Pouco espaço em disco – atente para limpeza."
    else:
        message = "Espaço em disco crítico – risco de falha de backups."

    duration_ms = (time.perf_counter() - started) * 1000
    log_event(
        "health.backup_disk",
        "INFO" if status == "ok" else "WARNING" if status == "warning" else "ERROR",
        status=status,
        duration_ms=round(duration_ms, 2),
        total_bytes=total,
        used_bytes=used,
        free_bytes=free,
        free_ratio=round(free_ratio, 4),
        path=backup_root,
    )

    return {
        "name": "Espaço em disco (backups)",
        "status": status,
        "message": message,
        "duration_ms": round(duration_ms, 2),
        "details": {
            "path": backup_root,
            "total_bytes": total,
            "used_bytes": used,
            "free_bytes": free,
            "free_ratio": free_ratio,
        },
    }


def run_health_checks() -> Dict[str, Any]:
    """
    Executa todos os checks e retorna um payload único:
    {
        "status": "ok|degraded|error",
        "checks": { ... }
    }
    """
    checks = {
        "database": check_database(),
        "google_credentials": check_google_credentials(),
        "backup_disk": check_backup_disk(),
    }

    statuses = {c["status"] for c in checks.values()}

    if "error" in statuses:
        global_status = "error"
    elif "warning" in statuses:
        global_status = "degraded"
    else:
        global_status = "ok"

    log_event("health.all", "INFO", global_status=global_status)

    return {
        "status": global_status,
        "checks": checks,
    }
