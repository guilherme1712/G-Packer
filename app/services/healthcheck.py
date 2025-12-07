import os
import shutil
import time
import json
import requests
from datetime import datetime
from typing import Dict, Any

from flask import current_app
from sqlalchemy import text

# Tenta importar psutil para métricas de servidor (CPU/RAM)
try:
    import psutil
except ImportError:
    psutil = None

from app.models import db
from app.models.google_auth import GoogleAuthModel
from app.models.backup_file import BackupFileModel
from app.models.task import TaskModel

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
except ImportError:
    Credentials = None
    Request = None


# ==========================================
# 1. MONITORAMENTO DE SISTEMA (CPU/RAM)
# ==========================================
def check_system_resources() -> Dict[str, Any]:
    """Monitora uso de CPU e Memória do servidor."""
    started = time.perf_counter()

    if not psutil:
        return {"status": "warning", "message": "Lib 'psutil' não instalada."}

    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()

        # Regras de Alerta
        if cpu_percent > 90 or mem.percent > 90:
            status = "error"
            msg = f"SOBRECARGA: CPU {cpu_percent}% / RAM {mem.percent}%"
        elif cpu_percent > 70 or mem.percent > 80:
            status = "warning"
            msg = f"Carga Alta: CPU {cpu_percent}% / RAM {mem.percent}%"
        else:
            status = "ok"
            msg = f"Estável. CPU: {cpu_percent}% | RAM: {mem.percent}%"

        return {
            "status": status,
            "message": msg,
            "details": {
                "cpu_usage": f"{cpu_percent}%",
                "ram_usage": f"{mem.percent}%",
                "ram_available": f"{mem.available / (1024 ** 3):.1f} GB"
            },
            "duration_ms": round((time.perf_counter() - started) * 1000, 2)
        }
    except Exception as e:
        return {"status": "error", "message": f"Erro ao ler sistema: {e}"}


# ==========================================
# 2. CONECTIVIDADE EXTERNA (INTERNET)
# ==========================================
def check_internet_connectivity() -> Dict[str, Any]:
    """Verifica se o servidor tem saída para a internet (Ping Google)."""
    started = time.perf_counter()
    try:
        # Timeout curto de 3s
        response = requests.get("https://www.google.com", timeout=3)
        duration_ms = (time.perf_counter() - started) * 1000

        if response.status_code == 200:
            status = "ok"
            msg = "Conexão Internet OK."
            if duration_ms > 1000:
                status = "warning"
                msg = "Internet lenta (>1s)."
        else:
            status = "warning"
            msg = f"Status inesperado: {response.status_code}"

        return {
            "status": status,
            "message": msg,
            "duration_ms": round(duration_ms, 2)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": "Sem conexão com Internet.",
            "duration_ms": 0.0,
            "details": {"error": str(e)}
        }


# ==========================================
# 3. BANCO DE DADOS (Conexão e Tamanho)
# ==========================================
def check_database_extended() -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        # Teste de conexão
        db.session.execute(text("SELECT 1"))

        # Teste de Tamanho do Arquivo (SQLite)
        db_uri = current_app.config.get("SQLALCHEMY_DATABASE_URI", "")
        db_size_mb = 0
        msg_extra = ""

        if db_uri.startswith("sqlite:///"):
            path = db_uri.replace("sqlite:///", "")
            if os.path.exists(path):
                size = os.path.getsize(path)
                db_size_mb = size / (1024 * 1024)
                msg_extra = f" | Tamanho: {db_size_mb:.1f} MB"

        duration_ms = (time.perf_counter() - started) * 1000

        # Alerta se o banco estiver gigante (> 500MB para SQLite é alerta)
        if db_size_mb > 500:
            return {
                "status": "warning",
                "message": f"Banco muito grande ({db_size_mb:.1f} MB).",
                "duration_ms": round(duration_ms, 2)
            }

        return {
            "status": "ok",
            "message": f"Operacional{msg_extra}",
            "duration_ms": round(duration_ms, 2)
        }
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Falha no BD: {str(exc)}",
            "duration_ms": 0.0
        }


# ==========================================
# 4. CREDENCIAIS GOOGLE (Via Banco)
# ==========================================
def check_google_auth_db() -> Dict[str, Any]:
    started = time.perf_counter()
    if not Credentials:
        return {"status": "warning", "message": "Libs Google ausentes."}

    try:
        auth = GoogleAuthModel.query.filter_by(active=True).order_by(GoogleAuthModel.updated_at.desc()).first()
        if not auth:
            return {"status": "error", "message": "Não conectado ao Google Drive."}

        data = json.loads(auth.token_json)
        creds = Credentials.from_authorized_user_info(data, data.get("scopes"))

        if creds.valid:
            status = "ok"
            msg = f"Conectado: {auth.email}"
        elif creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                status = "ok"
                msg = f"Token renovado: {auth.email}"
            except:
                status = "error"
                msg = "Falha ao renovar token."
        else:
            status = "error"
            msg = "Token inválido/revogado."

        return {
            "status": status,
            "message": msg,
            "duration_ms": round((time.perf_counter() - started) * 1000, 2)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==========================================
# 5. DISCO DE BACKUP
# ==========================================
def check_disk_space() -> Dict[str, Any]:
    started = time.perf_counter()
    backup_dir = current_app.config.get("BACKUP_STORAGE_DIR", ".")
    target = backup_dir if os.path.exists(backup_dir) else "."

    try:
        usage = shutil.disk_usage(target)
        free_gb = usage.free / (1024 ** 3)
        ratio = usage.free / usage.total

        if ratio < 0.10:
            status, msg = "error", f"CRÍTICO: {free_gb:.1f}GB livres."
        elif ratio < 0.20:
            status, msg = "warning", f"Baixo: {free_gb:.1f}GB livres."
        else:
            status, msg = "ok", f"Saudável: {free_gb:.1f}GB livres."

        return {
            "status": status,
            "message": msg,
            "details": {"path": target, "percent_free": f"{ratio * 100:.1f}%"},
            "duration_ms": round((time.perf_counter() - started) * 1000, 2)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==========================================
# 6. HISTÓRICO DE TAREFAS (FALHAS)
# ==========================================
def check_tasks_health() -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        # Pega as últimas 10 tarefas
        recent_tasks = TaskModel.query.order_by(TaskModel.created_at.desc()).limit(10).all()
        if not recent_tasks:
            return {"status": "ok", "message": "Nenhuma tarefa recente.", "duration_ms": 0}

        fail_count = sum(1 for t in recent_tasks if t.phase == 'erro' or t.errors_count > 0)

        if fail_count >= 3:
            status, msg = "error", f"Alerta: {fail_count} falhas nas últimas 10 execuções."
        elif fail_count > 0:
            status, msg = "warning", f"Atenção: {fail_count} falhas recentes."
        else:
            status, msg = "ok", "Últimas 10 execuções sem erros."

        return {
            "status": status,
            "message": msg,
            "duration_ms": round((time.perf_counter() - started) * 1000, 2)
        }
    except Exception as e:
        return {"status": "warning", "message": "Erro ao ler histórico tasks."}


# ==========================================
# FUNÇÃO PRINCIPAL
# ==========================================
def run_health_checks() -> Dict[str, Any]:
    checks = {
        "system": check_system_resources(),  # Novo
        "internet": check_internet_connectivity(),  # Novo
        "database": check_database_extended(),  # Melhorado
        "google_auth": check_google_auth_db(),
        "disk": check_disk_space(),
        "tasks": check_tasks_health()  # Novo
    }

    # Status global: Se um for erro = erro. Se um for warning = warning.
    statuses = [v["status"] for v in checks.values()]
    if "error" in statuses:
        global_status = "error"
    elif "warning" in statuses:
        global_status = "degraded"
    else:
        global_status = "ok"

    return {"status": global_status, "checks": checks, "timestamp": time.time()}