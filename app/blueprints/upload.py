import os
import uuid
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, current_app
from werkzeug.utils import secure_filename
from app.models import db, UploadHistoryModel, TaskModel
from app.services.auth import get_credentials
from app.services.Google.drive_upload import DriveUploadService
from app.services.queue_service import QueueService
from app.services.audit import AuditService
from app.enum.task_type import TaskTypeEnum

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

@upload_bp.route("/api/upload/batch-status", methods=["POST"])
def api_batch_status():
    data = request.json or {}
    task_ids = data.get('task_ids', [])

    if not task_ids:
        return jsonify({"percent": 0, "total_files": 0, "processed": 0, "success": 0, "error": 0})

    batch_id = task_ids[0]
    task = db.session.get(TaskModel, batch_id)
    if not task:
        return jsonify({"percent": 0, "total_files": 0, "processed": 0})

    total = task.files_total or 0
    success = task.files_downloaded or 0
    error = task.errors_count or 0
    processed = success + error
    
    percent = 0.0
    if total > 0:
        percent = (processed / total) * 100

    return jsonify({
        "total_files": total,
        "processed": processed,
        "success": success,
        "error": error,
        "percent": round(percent, 1),
        "speed_mb_s": 0.0,
        "bytes_total": task.bytes_found,
        "bytes_downloaded": task.bytes_downloaded
    })

@upload_bp.route("/api/upload/create-batch", methods=["POST"])
def api_create_batch():
    data = request.json
    total_files = data.get('total_files', 0)
    total_bytes = data.get('total_bytes', 0)
    
    task_id = f"batch_{uuid.uuid4().hex}"
    
    task = TaskModel(
        id=task_id,
        type=TaskTypeEnum.UPLOAD,
        phase="processando",
        message="Iniciando upload em lote...",
        files_found=total_files,
        files_total=total_files,
        files_downloaded=0,
        bytes_found=total_bytes,
        bytes_downloaded=0,
        canceled=False,
        paused=False
    )
    db.session.add(task)
    db.session.commit()
    return jsonify({"ok": True, "task_id": task_id})

@upload_bp.route("/api/upload/enqueue", methods=["POST"])
def api_enqueue():
    # --- ESTRATÉGIA ROBUSTA DE CAPTURA DE ARQUIVOS ---
    # Tenta pegar 'files' (padrão)
    files = request.files.getlist("files")
    
    # Se vazio, tenta 'files[]' (alguns browsers forçam isso)
    if not files:
        files = request.files.getlist("files[]")
        
    # Se ainda vazio, tenta 'file' (legado ou erro de frontend)
    if not files:
        files = request.files.getlist("file")

    # Captura os caminhos relativos
    raw_relative_paths = request.form.getlist("relative_paths")
    if not raw_relative_paths:
        raw_relative_paths = request.form.getlist("relative_paths[]") # Tenta com colchetes

    target_root_id = request.form.get("target_root_id")
    task_id = request.form.get("task_id")
    raw_preserve = request.form.get("preserve_structure")
    
    preserve_structure = (str(raw_preserve).strip().lower() == 'true')

    if not files:
        # Se cair aqui, realmente não chegou nada
        return jsonify({
            "ok": False, 
            "msg": "No files received", 
            "keys_received": list(request.files.keys())
        }), 400

    created_ids = []
    user_email = AuditService.get_current_user_email()
    
    temp_dir = os.path.join(current_app.root_path, '..', 'storage', 'queue')
    os.makedirs(temp_dir, exist_ok=True)

    for i, file in enumerate(files):
        if not file: continue

        # Sincroniza path. Se não tiver path correspondente, usa o filename.
        raw_relative_path = raw_relative_paths[i] if i < len(raw_relative_paths) else file.filename

        # 1. Salvar Temp
        safe_name = secure_filename(file.filename)
        unique_name = f"{uuid.uuid4().hex}_{safe_name}"
        save_path = os.path.join(temp_dir, unique_name)
        
        file.save(save_path)
        file_size = os.path.getsize(save_path)

        # 2. Define Caminho Final
        if preserve_structure and raw_relative_path and raw_relative_path != 'undefined':
            final_drive_path = raw_relative_path
        else:
            final_drive_path = file.filename

        # 3. DB Insert
        new_history = UploadHistoryModel(
            task_id=task_id,
            filename=file.filename,
            relative_path=final_drive_path,
            mime_type=file.mimetype,
            status='PENDING',
            destination_id=target_root_id,
            temp_path=save_path,
            size_bytes=file_size,
            user_email=user_email
        )
        db.session.add(new_history)
        db.session.flush()
        created_ids.append(new_history.id)

    db.session.commit()
    
    # Acorda o Worker
    QueueService.start_worker(current_app._get_current_object())

    return jsonify({"ok": True, "count": len(created_ids), "ids": created_ids})