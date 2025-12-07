# services/scheduler_service.py
import json
import time
import shutil
import os
import pytz

from datetime import datetime
from config import TIMEZONE

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.models import db, ScheduledTaskModel, BackupFileModel, BackupProfile, ScheduledRunModel
from .auth import get_credentials
from .drive_download import download_items_bundle
from .progress import PROGRESS, init_download_task

# Scheduler global
scheduler = BackgroundScheduler(
    timezone=pytz.timezone(TIMEZONE)
)
STORAGE_ROOT = os.path.join(os.getcwd(), "storage", "backups")

def _render_zip_pattern(pattern: str | None) -> str:
    """
    Renderiza o padrão de nome de arquivo do perfil (ex: backup-{YYYYMMDD})
    para um nome base de arquivo.
    """
    if not pattern:
        pattern = "backup-{YYYYMMDD}"

    now = datetime.now()
    replacements = {
        "{YYYYMMDD}": now.strftime("%Y%m%d"),
        "{YYYYMM}": now.strftime("%Y%m"),
        "{YYYY}": now.strftime("%Y"),
        "{YY}": now.strftime("%y"),
        "{MM}": now.strftime("%m"),
        "{DD}": now.strftime("%d"),
    }

    for token, value in replacements.items():
        pattern = pattern.replace(token, value)

    # sanitizer para nome de arquivo
    for ch in '\\/:*?"<>|':
        pattern = pattern.replace(ch, "_")

    return pattern


def log_upcoming_jobs():
    """
    Loga no console quando algum agendamento estiver prestes a rodar
    (por exemplo, dentro dos próximos 2 minutos).
    """
    now = datetime.now()
    threshold_seconds = 120  # 2 minutos

    for job in scheduler.get_jobs():
        # Ignora o próprio job de monitoramento, se existir
        if job.id == "upcoming_logger":
            continue

        next_run = job.next_run_time
        if not next_run:
            continue

        # normaliza para datetime "naive"
        if getattr(next_run, "tzinfo", None) is not None:
            next_run_naive = next_run.replace(tzinfo=None)
        else:
            next_run_naive = next_run

        delta = (next_run_naive - now).total_seconds()

        if 0 < delta <= threshold_seconds:
            print(
                f"[{now}] Aviso: agendamento ID {job.id} "
                f"será executado em breve (às {next_run_naive})."
            )


def job_executor(app_app_context, task_id_db):
    """
    Função que será executada pelo APScheduler.
    Passamos app_context porque o job roda em outra thread.
    """
    with app_app_context():
        start_ts = datetime.now()
        print(f"[{start_ts}] >>> EXECUTANDO AGENDAMENTO ID {task_id_db}...")

        task = ScheduledTaskModel.query.get(task_id_db)
        if not task or not task.active:
            print(f"[{datetime.now()}] Tarefa {task_id_db} não encontrada ou inativa. Abortando.")
            return

        # ---------------------------------------------------------
        # Origem da configuração: itens fixos x perfil de backup
        # ---------------------------------------------------------
        profile = None
        items = []
        base_zip_name = task.zip_name or "backup_agendado"

        if getattr(task, "profile_id", None):
            profile = BackupProfile.query.get(task.profile_id)
            if profile:
                # usa sempre a LISTA ATUAL de itens do perfil
                items = profile.items or []

                # se o perfil tiver nome específico, prioriza
                if profile.zip_name:
                    base_zip_name = profile.zip_name
                else:
                    # usa o padrão com data (backup-{YYYYMMDD}, etc)
                    base_zip_name = _render_zip_pattern(profile.zip_pattern)
            else:
                # perfil não encontrado -> fallback para items_json
                try:
                    items = json.loads(task.items_json)
                except Exception:
                    items = []
        else:
            try:
                items = json.loads(task.items_json)
            except Exception:
                items = []

        if not items:
            msg = "Nenhum item configurado para este agendamento."
            print(f"[{datetime.now()}] {msg}")
            task.last_status = msg
            task.last_run_at = datetime.now()
            db.session.commit()
            return

        print(
            f"[{datetime.now()}] Tarefa '{task.name}' "
            f"(freq={task.frequency}, hora={task.run_time}) iniciada."
        )

        creds = get_credentials()
        if not creds:
            print(f"[{datetime.now()}] ERRO: Credenciais inválidas ou expiradas.")
            task.last_status = "Erro: Credenciais expiradas"
            task.last_run_at = datetime.now()
            db.session.commit()
            return

        run_id = f"sched-{task.id}-{int(time.time())}"
        init_download_task(run_id)

        # Cria registro de histórico desta execução
        run_record = ScheduledRunModel(
            schedule_id=task.id,
            started_at=start_ts,
            status="Em execução",
        )
        db.session.add(run_record)
        db.session.commit()  # deixa visível enquanto estiver rodando

        try:
            date_str = datetime.now().strftime("%Y%m%d_%H%M")
            final_zip_name = f"{base_zip_name}_{date_str}"

            print(
                f"[{datetime.now()}] Iniciando mapeamento + download "
                f"para {len(items)} item(ns). Nome base: {final_zip_name}"
            )

            # Modo 'sequential' fixo, como antes
            zip_path = download_items_bundle(
                creds=creds,
                items=items,
                base_name=final_zip_name,
                compression_level="normal",
                archive_format="zip",
                progress_dict=PROGRESS,
                task_id=run_id,
                processing_mode="sequential",
            )

            print(f"[{datetime.now()}] Download concluído. Zip temporário em: {zip_path}")

            if not os.path.exists(STORAGE_ROOT):
                os.makedirs(STORAGE_ROOT)

            filename = os.path.basename(zip_path)
            final_dest = os.path.join(STORAGE_ROOT, filename)
            shutil.move(zip_path, final_dest)

            stat = os.stat(final_dest)
            size_mb = round(stat.st_size / (1024 * 1024), 2)

            bf = BackupFileModel(
                filename=filename,
                path=final_dest,
                size_mb=size_mb,
                items_count=len(items),
                origin_task_id=f"AUTO_{task.id}",
            )
            db.session.add(bf)

            task.last_status = f"Sucesso: {filename} ({size_mb} MB)"
            task.last_run_at = datetime.now()

            # Atualiza histórico desta execução
            run_record.finished_at = datetime.now()
            run_record.status = "Sucesso"
            run_record.size_mb = size_mb
            run_record.filename = filename

            print(
                f"[{datetime.now()}] <<< AGENDAMENTO ID {task.id} finalizado com sucesso. "
                f"Arquivo: {filename} ({size_mb} MB)"
            )

        except Exception as e:
            err = str(e)
            print(f"[{datetime.now()}] ERRO no agendamento ID {task.id}: {err}")
            task.last_status = f"Erro: {err[:100]}"
            task.last_run_at = datetime.now()

            run_record.finished_at = datetime.now()
            run_record.status = f"Erro: {err[:100]}"

        db.session.commit()


def init_scheduler(app):
    """
    Inicializa o scheduler e carrega as tarefas do banco.
    Deve ser chamado no startup do Flask.
    """
    if not scheduler.running:
        scheduler.start()
        print("[Scheduler] BackgroundScheduler iniciado.")

    reload_jobs(app)


def reload_jobs(app):
    """
    Limpa todos os jobs e recarrega do banco de dados.
    Chamado ao iniciar o app ou ao criar/editar tarefas.
    """
    print("[Scheduler] Recarregando jobs agendados...")
    scheduler.remove_all_jobs()

    with app.app_context():
        tasks = ScheduledTaskModel.query.filter_by(active=True).all()
        print(f"[Scheduler] Encontradas {len(tasks)} tarefas ativas para agendar.")

        for task in tasks:
            try:
                hour, minute = map(int, task.run_time.split(":"))
            except Exception:
                hour, minute = 0, 0

            trigger = None

            if task.frequency == "daily":
                trigger = CronTrigger(hour=hour, minute=minute, timezone=pytz.timezone(TIMEZONE))

            elif task.frequency == "weekly":
                # ex: toda segunda-feira (ajuste se quiser outro dia)
                trigger = CronTrigger(day_of_week="mon", hour=hour, minute=minute, timezone=pytz.timezone(TIMEZONE))

            elif task.frequency == "monthly":
                trigger = CronTrigger(day=1, hour=hour, minute=minute, timezone=pytz.timezone(TIMEZONE))

            if trigger:
                scheduler.add_job(
                    func=job_executor,
                    trigger=trigger,
                    args=[app.app_context, task.id],
                    id=str(task.id),
                    replace_existing=True,
                )
                print(
                    f"[Scheduler] -> Job '{task.name}' (id={task.id}) agendado: "
                    f"{task.frequency} às {task.run_time}"
                )

    # Job auxiliar que roda a cada 60s para avisar quando existir
    # agendamento próximo da hora de execução.
    scheduler.add_job(
        func=log_upcoming_jobs,
        trigger="interval",
        seconds=60,
        id="upcoming_logger",
        replace_existing=True,
    )
    print("[Scheduler] Job de monitoramento de agendamentos futuros registrado (a cada 60s).")
