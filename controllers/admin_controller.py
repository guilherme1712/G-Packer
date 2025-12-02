# controllers/admin_controller.py
import os
from datetime import datetime

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
from models import db, TaskModel, BackupProfile, BackupFileModel

admin_bp = Blueprint("admin", __name__)

BACKUP_FOLDER_NAME = "storage/backups"


@admin_bp.route("/admin/db")
def view_db():
    creds = get_credentials()
    if not creds:
        return "Acesso negado. Faça login primeiro.", 403

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).limit(50).all()
    profiles = BackupProfile.query.all()

    return render_template("db_view.html", tasks=tasks, profiles=profiles)


@admin_bp.route("/admin/api/tasks")
def api_tasks():
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "Unauthorized"}), 403

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).all()
    return jsonify([t.to_dict() for t in tasks])


def _sync_backups_from_disk():
    """
    Garante que todo .zip / .tar.gz existente na pasta de backups
    tenha um registro correspondente na tabela backup_files.

    Se encontrar arquivo físico sem registro, cria o registro
    com tamanho e data de modificação do arquivo.
    """
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(folder_path, exist_ok=True)

    # Mapear existentes no banco por filename
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
            # Já está no banco, só garante path atualizado se precisar
            b = existing_by_name[fname]
            if not b.path or b.path != full_path:
                b.path = full_path
                changed = True
            continue

        # Arquivo não tem registro ainda → cria
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
            # Não trava a página se der erro em algum arquivo
            print(f"Erro ao sincronizar arquivo de backup '{fname}' para o DB: {e}")

    if changed:
        try:
            db.session.commit()
        except Exception as e:
            print(f"Erro ao commit na sincronização de backups: {e}")
            db.session.rollback()


@admin_bp.route("/admin/backups")
def list_backups():
    """
    Lista backups a partir da tabela backup_files, garantindo antes que
    todos os .zip/.tar(.gz) presentes em disco tenham registro no DB.
    """
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    # 1) Sincroniza diretório físico -> banco
    _sync_backups_from_disk()

    # 2) Carrega lista do banco já sincronizado
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
    """
    Apaga o arquivo físico e o registro em backup_files.
    """
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    safe_filename = os.path.basename(filename)
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)

    # busca no banco
    backup = BackupFileModel.query.filter_by(filename=safe_filename).first()

    # path base: dentro da pasta de backups
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

    # remove do banco
    if backup:
        try:
            db.session.delete(backup)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            flash(f"Erro ao remover registro no banco: {e}")

    return redirect(url_for("admin.list_backups"))
