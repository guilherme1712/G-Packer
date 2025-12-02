import os
import time
from datetime import datetime
from flask import Blueprint, render_template, jsonify, current_app, flash, redirect, url_for
from services.auth_service import get_credentials
from models import TaskModel, BackupProfile

# Define o Blueprint
admin_bp = Blueprint("admin", __name__)

# Configuração da pasta onde os zips são salvos
BACKUP_FOLDER_NAME = 'storage/backups'

@admin_bp.route("/admin/db")
def view_db():
    """Renderiza uma página simples listando o conteúdo das tabelas (Tasks e Profiles)."""
    creds = get_credentials()
    if not creds:
        return "Acesso negado. Faça login primeiro.", 403

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).limit(50).all()
    profiles = BackupProfile.query.all()

    return render_template("db_view.html", tasks=tasks, profiles=profiles)

@admin_bp.route("/admin/api/tasks")
def api_tasks():
    """API JSON caso queira consumir os dados programaticamente."""
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "Unauthorized"}), 403

    tasks = TaskModel.query.order_by(TaskModel.updated_at.desc()).all()
    return jsonify([t.to_dict() for t in tasks])

# --- NOVAS ROTAS PARA GERENCIAR ARQUIVOS FÍSICOS ---

@admin_bp.route("/admin/backups")
def list_backups():
    """Lista os arquivos ZIP salvos no servidor."""
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    # Garante que a pasta existe
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(folder_path, exist_ok=True)

    files_data = []

    # Varre a pasta
    try:
        with os.scandir(folder_path) as entries:
            for entry in entries:
                if entry.is_file() and not entry.name.startswith('.'):
                    stat = entry.stat()
                    # Converte tamanho para MB
                    size_mb = round(stat.st_size / (1024 * 1024), 2)
                    # Data de modificação
                    dt = datetime.fromtimestamp(stat.st_mtime)

                    files_data.append({
                        'name': entry.name,
                        'path': entry.path,
                        'size_mb': size_mb,
                        'created_at': dt.strftime('%d/%m/%Y %H:%M:%S'),
                        'timestamp': stat.st_mtime
                    })
    except Exception as e:
        flash(f"Erro ao ler pasta de backups: {e}")

    # Ordena do mais recente para o mais antigo
    files_data.sort(key=lambda x: x['timestamp'], reverse=True)

    # Renderiza um template específico para lista de arquivos
    # (Você precisará criar admin_backups.html ou adaptar o db_view.html)
    return render_template("admin_backups.html", files=files_data)

@admin_bp.route("/admin/backups/delete/<filename>")
def delete_backup(filename):
    """Apaga um arquivo físico da pasta de backups."""
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    # Segurança básica de path traversal
    safe_filename = os.path.basename(filename)
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    file_path = os.path.join(folder_path, safe_filename)

    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            flash(f"Arquivo '{safe_filename}' removido com sucesso.")
        except Exception as e:
            flash(f"Erro ao remover arquivo: {e}")
    else:
        flash("Arquivo não encontrado.")

    return redirect(url_for('admin.list_backups'))
