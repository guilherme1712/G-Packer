import os
import uuid
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, current_app
from werkzeug.utils import secure_filename
from app.models import db, UploadHistoryModel
from app.services.auth import get_credentials
from app.services.Google.drive_upload import DriveUploadService
from app.services.queue_service import QueueService
from app.services.audit import AuditService

upload_bp = Blueprint("upload", __name__)

@upload_bp.route("/upload")
def index():
    creds = get_credentials()
    if not creds: return redirect(url_for('auth.login'))
    return render_template("upload.html")

@upload_bp.route("/api/drive/tree-nodes", methods=["GET"])
def api_tree_nodes():
    creds = get_credentials()
    if not creds: return jsonify({"ok": False, "error": "Auth required"}), 401
    
    parent_id = request.args.get("parent_id", "root")
    try:
        folders = DriveUploadService.list_folders(creds, parent_id)
        items = [{"id": f["id"], "name": f["name"], "type": "folder"} for f in folders]
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@upload_bp.route("/api/upload/global-status")
def api_global_status():
    stats = QueueService.get_global_progress()
    return jsonify(stats)

@upload_bp.route("/api/upload/enqueue", methods=["POST"])
def api_enqueue():
    file = request.files.get("file")
    relative_path = request.form.get("relative_path")
    target_root_id = request.form.get("target_root_id")
    
    # Conversão correta da string 'true'/'false' do JS para Booleano
    raw_preserve = request.form.get("preserve_structure")
    preserve_structure = (raw_preserve == 'true')
    
    if not file: return jsonify({"ok": False}), 400

    # 1. Salvar no Disco Local (Temp)
    safe_name = secure_filename(file.filename)
    temp_dir = os.path.join(current_app.root_path, '..', 'storage', 'queue')
    os.makedirs(temp_dir, exist_ok=True)
    
    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
    save_path = os.path.join(temp_dir, unique_name)
    file.save(save_path)
    file_size = os.path.getsize(save_path)

    # 2. Lógica da Estrutura de Pastas
    # Se preserve_structure é FALSE, ignoramos a pasta de origem e usamos apenas o nome do arquivo
    final_drive_path = relative_path if preserve_structure else file.filename

    # 3. Criar registro no Banco (PENDING)
    user_email = AuditService.get_current_user_email()
    
    new_task = UploadHistoryModel(
        filename=file.filename,
        relative_path=final_drive_path,
        mime_type=file.mimetype,
        status='PENDING',
        destination_id=target_root_id,
        temp_path=save_path,
        size_bytes=file_size,
        user_email=user_email
    )
    db.session.add(new_task)
    db.session.commit()

    # 4. Acordar o Worker
    QueueService.start_worker(current_app._get_current_object())

    return jsonify({"ok": True})