# services/scheduler_service.py
import json
import time
import shutil
import os
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import current_app

from models import db, ScheduledTaskModel, BackupFileModel
from services.auth_service import get_credentials
from services.drive_download_service import download_items_bundle
from services.progress_service import PROGRESS, init_download_task

# Scheduler global
scheduler = BackgroundScheduler()
STORAGE_ROOT = os.path.join(os.getcwd(), "storage", "backups")

def job_executor(app_app_context, task_id_db):
    """
    Função que será executada pelo APScheduler.
    Passamos app_context porque o job roda em outra thread.
    """
    with app_app_context():
        print(f"[{datetime.now()}] Iniciando Job para Tarefa ID {task_id_db}...")

        task = ScheduledTaskModel.query.get(task_id_db)
        if not task or not task.active:
            print(f"Tarefa {task_id_db} não encontrada ou inativa.")
            return

        creds = get_credentials()
        if not creds:
            print("ERRO: Credenciais inválidas.")
            task.last_status = "Erro: Credenciais expiradas"
            db.session.commit()
            return

        run_id = f"sched-{task.id}-{int(time.time())}"
        init_download_task(run_id)

        try:
            items = json.loads(task.items_json)
            date_str = datetime.now().strftime("%Y%m%d_%H%M")
            final_zip_name = f"{task.zip_name}_{date_str}"

            zip_path = download_items_bundle(
                creds=creds,
                items=items,
                base_name=final_zip_name,
                compression_level="normal",
                archive_format="zip",
                progress_dict=PROGRESS,
                task_id=run_id,
                processing_mode="concurrent"
            )

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
                origin_task_id=f"AUTO_{task.id}"
            )
            db.session.add(bf)

            task.last_status = f"Sucesso: {filename} ({size_mb} MB)"
            task.last_run_at = datetime.now()

            print(f"Job {task.id} finalizado com sucesso.")

        except Exception as e:
            err = str(e)
            print(f"Erro no Job {task.id}: {err}")
            task.last_status = f"Erro: {err[:100]}"

        db.session.commit()


def init_scheduler(app):
    """
    Inicializa o scheduler e carrega as tarefas do banco.
    Deve ser chamado no startup do Flask.
    """
    if not scheduler.running:
        scheduler.start()

    reload_jobs(app)


def reload_jobs(app):
    """
    Limpa todos os jobs e recarrega do banco de dados.
    Chamado ao iniciar o app ou ao criar/editar tarefas.
    """
    scheduler.remove_all_jobs()

    with app.app_context():
        tasks = ScheduledTaskModel.query.filter_by(active=True).all()
        print(f"Carregando {len(tasks)} tarefas agendadas...")

        for task in tasks:
            try:
                hour, minute = map(int, task.run_time.split(':'))
            except Exception:
                hour, minute = 0, 0

            trigger = None

            if task.frequency == 'daily':
                trigger = CronTrigger(hour=hour, minute=minute)

            elif task.frequency == 'weekly':
                # ex: toda segunda-feira
                trigger = CronTrigger(day_of_week='mon', hour=hour, minute=minute)

            elif task.frequency == 'monthly':
                trigger = CronTrigger(day=1, hour=hour, minute=minute)

            if trigger:
                scheduler.add_job(
                    func=job_executor,
                    trigger=trigger,
                    args=[app.app_context, task.id],
                    id=str(task.id),
                    replace_existing=True
                )
                print(f" -> Job {task.name} agendado ({task.frequency} às {task.run_time})")
