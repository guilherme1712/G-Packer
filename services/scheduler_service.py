# services/scheduler_service.py
import json
import time
import shutil
import os
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from models import db, ScheduledTaskModel, BackupFileModel
from services.auth_service import get_credentials
from services.drive_download_service import download_items_bundle
from services.progress_service import PROGRESS, init_download_task

# Scheduler global
scheduler = BackgroundScheduler()
STORAGE_ROOT = os.path.join(os.getcwd(), "storage", "backups")


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

        print(
            f"[{datetime.now()}] Tarefa '{task.name}' "
            f"(freq={task.frequency}, hora={task.run_time}) iniciada."
        )

        creds = get_credentials()
        if not creds:
            print(f"[{datetime.now()}] ERRO: Credenciais inválidas ou expiradas.")
            task.last_status = "Erro: Credenciais expiradas"
            db.session.commit()
            return

        run_id = f"sched-{task.id}-{int(time.time())}"
        init_download_task(run_id)

        try:
            items = json.loads(task.items_json)
            date_str = datetime.now().strftime("%Y%m%d_%H%M")
            final_zip_name = f"{task.zip_name}_{date_str}"

            print(
                f"[{datetime.now()}] Iniciando mapeamento + download "
                f"para {len(items)} item(ns). Nome base: {final_zip_name}"
            )

            # IMPORTANTE: modo 'sequential' -> mapeia tudo primeiro, depois baixa,
            # exatamente como o usuário pediu (mesmo comportamento do modal).
            zip_path = download_items_bundle(
                creds=creds,
                items=items,
                base_name=final_zip_name,
                compression_level="normal",
                archive_format="zip",
                progress_dict=PROGRESS,
                task_id=run_id,
                processing_mode="sequential",   # <<< aqui troca de concurrent -> sequential
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

            print(
                f"[{datetime.now()}] <<< AGENDAMENTO ID {task.id} finalizado com sucesso. "
                f"Arquivo: {filename} ({size_mb} MB)"
            )

        except Exception as e:
            err = str(e)
            print(f"[{datetime.now()}] ERRO no agendamento ID {task.id}: {err}")
            task.last_status = f"Erro: {err[:100]}"

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
                trigger = CronTrigger(hour=hour, minute=minute)

            elif task.frequency == "weekly":
                # ex: toda segunda-feira (ajuste se quiser outro dia)
                trigger = CronTrigger(day_of_week="mon", hour=hour, minute=minute)

            elif task.frequency == "monthly":
                trigger = CronTrigger(day=1, hour=hour, minute=minute)

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
