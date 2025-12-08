# app/services/healthcheck.py
import os
import shutil
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any

import requests
from flask import current_app
from sqlalchemy import text, func

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
        return {
            "status": "warning",
            "message": "Lib 'psutil' não instalada.",
            "details": {},
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }

    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()

        # Regras de Alerta
        if cpu_percent > 90 or mem.percent > 90:
            status = "error"
            msg = f"SOBRECARGA: CPU {cpu_percent:.0f}% / RAM {mem.percent:.0f}%"
        elif cpu_percent > 70 or mem.percent > 80:
            status = "warning"
            msg = f"Carga Alta: CPU {cpu_percent:.0f}% / RAM {mem.percent:.0f}%"
        else:
            status = "ok"
            msg = f"Estável. CPU: {cpu_percent:.0f}% | RAM: {mem.percent:.0f}%"

        return {
            "status": status,
            "message": msg,
            "details": {
                "cpu_usage": f"{cpu_percent:.1f}%",
                "ram_usage": f"{mem.percent:.1f}%",
                "ram_available": f"{mem.available / (1024 ** 3):.1f} GB",
            },
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Erro ao ler sistema: {e}",
            "details": {},
            "duration_ms": 0.0,
        }


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
            "details": {},
            "duration_ms": round(duration_ms, 2),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": "Sem conexão com Internet.",
            "details": {"error": str(e)},
            "duration_ms": 0.0,
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
                "details": {"size_mb": f"{db_size_mb:.1f}"},
                "duration_ms": round(duration_ms, 2),
            }

        return {
            "status": "ok",
            "message": f"Operacional{msg_extra}",
            "details": {"size_mb": f"{db_size_mb:.1f}"},
            "duration_ms": round(duration_ms, 2),
        }
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Falha no BD: {str(exc)}",
            "details": {},
            "duration_ms": 0.0,
        }


# ==========================================
# 4. CREDENCIAIS GOOGLE (Via Banco)
# ==========================================
def check_google_auth_db() -> Dict[str, Any]:
    started = time.perf_counter()
    if not Credentials:
        return {
            "status": "warning",
            "message": "Libs Google ausentes.",
            "details": {},
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }

    try:
        auth = (
            GoogleAuthModel.query.filter_by(active=True)
            .order_by(GoogleAuthModel.updated_at.desc())
            .first()
        )
        if not auth:
            return {
                "status": "error",
                "message": "Não conectado ao Google Drive.",
                "details": {},
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }

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
            except Exception:
                status = "error"
                msg = "Falha ao renovar token."
        else:
            status = "error"
            msg = "Token inválido/revogado."

        return {
            "status": status,
            "message": msg,
            "details": {"email": auth.email},
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "details": {},
            "duration_ms": 0.0,
        }


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
        used_percent = 100 - (ratio * 100)

        if ratio < 0.10:
            status, msg = "error", f"CRÍTICO: {free_gb:.1f}GB livres."
        elif ratio < 0.20:
            status, msg = "warning", f"Baixo: {free_gb:.1f}GB livres."
        else:
            status, msg = "ok", f"Saudável: {free_gb:.1f}GB livres."

        return {
            "status": status,
            "message": msg,
            "details": {
                "path": target,
                "percent_free": f"{ratio * 100:.1f}%",
                "percent_used": f"{used_percent:.1f}%",
            },
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "details": {},
            "duration_ms": 0.0,
        }


# ==========================================
# 6. HISTÓRICO DE TAREFAS (FALHAS)
# ==========================================
def check_tasks_health() -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        # Pega as últimas 10 tarefas
        recent_tasks = (
            TaskModel.query.order_by(TaskModel.created_at.desc()).limit(10).all()
        )
        if not recent_tasks:
            return {
                "status": "ok",
                "message": "Nenhuma tarefa recente.",
                "details": {},
                "duration_ms": 0.0,
            }

        fail_count = sum(
            1 for t in recent_tasks if t.phase == "erro" or (t.errors_count or 0) > 0
        )
        success_count = len(recent_tasks) - fail_count

        if fail_count >= 3:
            status, msg = (
                "error",
                f"Alerta: {fail_count} falhas nas últimas 10 execuções.",
            )
        elif fail_count > 0:
            status, msg = (
                "warning",
                f"Atenção: {fail_count} falhas recentes.",
            )
        else:
            status, msg = "ok", "Últimas 10 execuções sem erros."

        return {
            "status": status,
            "message": msg,
            "details": {
                "recent_failures": fail_count,
                "recent_success": success_count,
                "total_recent": len(recent_tasks),
            },
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception:
        return {
            "status": "warning",
            "message": "Erro ao ler histórico tasks.",
            "details": {},
            "duration_ms": 0.0,
        }


# ==========================================
# 7. MÉTRICAS AVANÇADAS (jobs, falhas, compressão, throughput)
# ==========================================
def build_dashboard_metrics() -> Dict[str, Any]:
    """
    Coleta métricas adicionais para o dashboard em tempo real:
      - Jobs pendentes / ativos / concluídos 24h / agendados
      - Histórico de falhas (últimos 7 dias)
      - Taxa de compressão média / última
      - Velocidades de download (MB/s)
    """
    now = datetime.utcnow()
    last_24h = now - timedelta(hours=24)
    last_7d = now - timedelta(days=7)

    # ---- Jobs ----
    running_phases = [
        "mapeando",
        "baixando",
        "compactando",
        "espelhando",
        "processando",
    ]
    pending_phases = ["iniciando", "fila", "aguardando"]

    running = (
        TaskModel.query.filter(TaskModel.phase.in_(running_phases)).count()
    )
    pending = (
        TaskModel.query.filter(TaskModel.phase.in_(pending_phases)).count()
    )
    finished_24h = (
        TaskModel.query.filter(TaskModel.updated_at >= last_24h)
        .filter(TaskModel.phase == "concluido")
        .count()
    )
    failed_24h = (
        TaskModel.query.filter(TaskModel.updated_at >= last_24h)
        .filter(
            (TaskModel.phase == "erro")
            | (TaskModel.errors_count.isnot(None) & (TaskModel.errors_count > 0))
        )
        .count()
    )

    # Se você quiser incluir jobs agendados, pode ler diretamente a tabela de agendamentos
    # para não acoplar com o controller: usamos SQL cru no scheduler se necessário.
    # Aqui mantemos só o total de tasks no banco.
    total_tasks = TaskModel.query.count()

    jobs = {
        "running": running,
        "pending": pending,
        "finished_24h": finished_24h,
        "failed_24h": failed_24h,
        "total_tasks": total_tasks,
    }

    # ---- Falhas (últimos 7 dias) ----
    recent_failures = (
        TaskModel.query.filter(TaskModel.created_at >= last_7d)
        .filter(
            (TaskModel.phase == "erro")
            | (TaskModel.errors_count.isnot(None) & (TaskModel.errors_count > 0))
        )
        .all()
    )

    by_day: dict[str, int] = {}
    for t in recent_failures:
        if not t.created_at:
            continue
        day = t.created_at.date().isoformat()
        by_day[day] = by_day.get(day, 0) + 1

    failures = {
        "total_last_7_days": len(recent_failures),
        "by_day": [
            {"day": day, "failures": count}
            for day, count in sorted(by_day.items())
        ],
    }

    # ---- Compressão / Throughput ----
    # Pega últimos 20 arquivos de backup com task associada
    backups = (
        BackupFileModel.query.order_by(BackupFileModel.created_at.desc())
        .limit(20)
        .all()
    )

    compression_ratios: list[float] = []
    speeds: list[float] = []

    for b in backups:
        if not b.origin_task_id:
            continue

        task = TaskModel.query.get(b.origin_task_id)
        if not task:
            continue

        # Taxa de compressão: original_mb / compactado_mb
        if task.bytes_found and b.size_mb:
            original_mb = task.bytes_found / (1024 * 1024)
            if original_mb > 0:
                compression_ratios.append(original_mb / float(b.size_mb))

        # Velocidade: bytes_downloaded / delta tempo
        if task.bytes_downloaded and task.created_at and task.updated_at:
            delta = task.updated_at - task.created_at
            seconds = max(delta.total_seconds(), 1.0)
            mb = task.bytes_downloaded / (1024 * 1024)
            speeds.append(mb / seconds)

    def _avg(values: list[float]) -> float:
        return round(sum(values) / len(values), 2) if values else 0.0

    backups_metrics = {
        "avg_compression_ratio": _avg(compression_ratios),
        "last_compression_ratio": round(compression_ratios[0], 2)
        if compression_ratios
        else 0.0,
    }

    throughput = {
        "avg_download_speed_mb_s": _avg(speeds),
        "last_download_speed_mb_s": round(speeds[0], 2) if speeds else 0.0,
    }

    return {
        "jobs": jobs,
        "failures": failures,
        "backups": backups_metrics,
        "throughput": throughput,
    }


# ==========================================
# 8. FUNÇÃO PRINCIPAL
# ==========================================
def run_health_checks() -> Dict[str, Any]:
    checks = {
        "system": check_system_resources(),
        "internet": check_internet_connectivity(),
        "database": check_database_extended(),
        "google_auth": check_google_auth_db(),
        "disk": check_disk_space(),
        "tasks": check_tasks_health(),
    }

    # Status global: Se um for erro = erro. Se um for warning = warning.
    statuses = [v["status"] for v in checks.values()]
    if "error" in statuses:
        global_status = "error"
    elif "warning" in statuses:
        global_status = "degraded"
    else:
        global_status = "ok"

    metrics = build_dashboard_metrics()

    return {
        "status": global_status,
        "checks": checks,
        "metrics": metrics,
        "timestamp": time.time(),
    }
