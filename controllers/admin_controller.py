# controllers/admin_controller.py
import os
import json

from datetime import datetime
from sqlalchemy import text, inspect

from flask import (
    Blueprint,
    render_template,
    jsonify,
    current_app,
    flash,
    redirect,
    url_for,
)

from services.auth_service import get_credentials
from models import db, TaskModel, BackupProfile, BackupFileModel, ScheduledTaskModel, ScheduledRunModel, GoogleAuthModel

admin_bp = Blueprint("admin", __name__)

BACKUP_FOLDER_NAME = "storage/backups"


def _run_auto_migrations():
    """
    Verifica se o esquema do banco SQLite está atualizado com as novas colunas.
    Se não estiver, aplica os comandos ALTER TABLE necessários.
    """
    try:
        inspector = inspect(db.engine)
        existing_tables = inspector.get_table_names()

        # --- Migração Tabela TASKS ---
        if 'tasks' in existing_tables:
            columns = [c['name'] for c in inspector.get_columns('tasks')]

            with db.engine.connect() as conn:
                if 'paused' not in columns:
                    conn.execute(text("ALTER TABLE tasks ADD COLUMN paused BOOLEAN DEFAULT 0"))
                    conn.commit()

                if 'canceled' not in columns:
                    conn.execute(text("ALTER TABLE tasks ADD COLUMN canceled BOOLEAN DEFAULT 0"))
                    conn.commit()

        # --- Migração Tabela BACKUP_PROFILES ---
        if 'backup_profiles' in existing_tables:
            columns = [c['name'] for c in inspector.get_columns('backup_profiles')]

            with db.engine.connect() as conn:
                if 'zip_name' not in columns:
                    conn.execute(text("ALTER TABLE backup_profiles ADD COLUMN zip_name VARCHAR(150)"))
                    conn.commit()

                if 'execution_mode' not in columns:
                    conn.execute(text("ALTER TABLE backup_profiles ADD COLUMN execution_mode VARCHAR(20) DEFAULT 'immediate'"))
                    conn.commit()

                if 'processing_mode' not in columns:
                    # Default sequential para garantir comportamento estável
                    print("AUTOFIX: Adicionando coluna 'processing_mode' em 'backup_profiles'...")
                    conn.execute(text("ALTER TABLE backup_profiles ADD COLUMN processing_mode VARCHAR(20) DEFAULT 'sequential'"))
                    conn.commit()

    except Exception as e:
        print(f"Aviso: Erro ao tentar auto-migrar o banco: {e}")


@admin_bp.route("/admin/db")
def view_db():
    creds = get_credentials()
    if not creds:
        return "Acesso negado. Faça login primeiro.", 403

    _run_auto_migrations()

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).limit(50).all()
    profiles = BackupProfile.query.all()

    sched_tasks = ScheduledTaskModel.query.order_by(
        ScheduledTaskModel.active.desc(),
        ScheduledTaskModel.next_run_at.asc()
    ).all()

    # calcula a quantidade de itens de cada agendamento + histórico
    runs_by_schedule = {}

    for s in sched_tasks:
        try:
            s.items_count = len(json.loads(s.items_json)) if s.items_json else 0
        except Exception:
            s.items_count = 0

        try:
            runs_by_schedule[s.id] = (
                s.runs.order_by(ScheduledRunModel.started_at.desc())
                .limit(10)
                .all()
            )
        except Exception:
            runs_by_schedule[s.id] = []


    google_auth = (
        GoogleAuthModel.query
        .filter_by(active=True)
        .order_by(GoogleAuthModel.updated_at.desc())
        .first()
    )

    return render_template(
        "db_view.html",
        tasks=tasks,
        profiles=profiles,
        sched_tasks=sched_tasks,
        runs_by_schedule=runs_by_schedule,
        google_auth=google_auth,
    )


@admin_bp.route("/admin/api/tasks")
def api_tasks():
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "Unauthorized"}), 403

    _run_auto_migrations()

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).all()
    return jsonify([t.to_dict() for t in tasks])


def _sync_backups_from_disk():
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(folder_path, exist_ok=True)

    existing_by_name = {
        b.filename: b for b in BackupFileModel.query.all()
    }

    changed = False

    for fname in os.listdir(folder_path):
        full_path = os.path.join(folder_path, fname)
        if not os.path.isfile(full_path):
            continue

        lower = fname.lower()
        if not (lower.endswith(".zip") or lower.endswith(".tar.gz") or lower.endswith(".tar")):
            continue

        if fname in existing_by_name:
            b = existing_by_name[fname]
            if not b.path or b.path != full_path:
                b.path = full_path
                changed = True
            continue

        try:
            stat = os.stat(full_path)
            size_mb = stat.st_size / (1024 * 1024)
            created_at = datetime.fromtimestamp(stat.st_mtime)

            bf = BackupFileModel(
                filename=fname,
                path=full_path,
                size_mb=size_mb,
                created_at=created_at,
                items_count=0,
                origin_task_id=None,
            )
            db.session.add(bf)
            changed = True
        except Exception as e:
            print(f"Erro ao sincronizar arquivo de backup '{fname}' para o DB: {e}")

    if changed:
        try:
            db.session.commit()
        except Exception as e:
            print(f"Erro ao commit na sincronização de backups: {e}")
            db.session.rollback()


@admin_bp.route("/admin/backups")
def list_backups():
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    _sync_backups_from_disk()

    backups = BackupFileModel.query.order_by(
        BackupFileModel.created_at.desc()
    ).all()

    files = []
    for b in backups:
        files.append(
            {
                "name": b.filename,
                "path": b.path,
                "size_mb": round(b.size_mb or 0.0, 2),
                "created_at": b.created_at.strftime("%d/%m/%Y %H:%M:%S")
                if b.created_at
                else "",
                "items_count": b.items_count or 0,
                "origin_task_id": b.origin_task_id,
            }
        )

    return render_template("admin_backups.html", files=files)


@admin_bp.route("/admin/backups/delete/<filename>")
def delete_backup(filename):
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    safe_filename = os.path.basename(filename)
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)

    backup = BackupFileModel.query.filter_by(filename=safe_filename).first()

    file_path = os.path.join(folder_path, safe_filename)
    if backup and backup.path:
        file_path = backup.path

    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            flash(f"Arquivo '{safe_filename}' removido com sucesso.")
        except Exception as e:
            flash(f"Erro ao remover arquivo: {e}")
    else:
        flash("Arquivo físico não encontrado.")

    if backup:
        try:
            db.session.delete(backup)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            flash(f"Erro ao remover registro no banco: {e}")

    return redirect(url_for("admin.list_backups"))