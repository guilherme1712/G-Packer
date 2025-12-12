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

@upload_bp.route("/api/upload/global-status")
def api_global_status():
    stats = QueueService.get_global_progress()
    return jsonify(stats)

# app/blueprints/upload.py

# ... imports existentes ...

@upload_bp.route("/api/upload/create-batch", methods=["POST"])
def api_create_batch():
    """
    Cria a TASK PAI única para todo o lote de arquivos.
    """
    data = request.json
    total_files = data.get('total_files', 0)
    total_bytes = data.get('total_bytes', 0)
    
    # Gera um ID único para o LOTE (Task)
    task_id = f"batch_{uuid.uuid4().hex}"
    user_email = AuditService.get_current_user_email()

    # Cria a Task Pai com o total de arquivos esperado
    task = TaskModel(
        id=task_id,
        type=TaskTypeEnum.UPLOAD,
        phase="processando", # ou 'enfileirando'
        message="Iniciando upload em lote...",
        files_found=total_files,
        files_total=total_files,
        files_downloaded=0, # Usaremos este campo para contar arquivos enviados
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
    file = request.files.get("file")
    
    # Recebe os dados brutos
    raw_relative_path = request.form.get("relative_path") 
    target_root_id = request.form.get("target_root_id")
    task_id = request.form.get("task_id")
    
    # Tratamento rigoroso do Booleano
    raw_preserve = request.form.get("preserve_structure")
    # O JS envia a string "true" se marcado, ou "false"/null se não.
    preserve_structure = (str(raw_preserve).lower() == 'true')

    if not file:
        return jsonify({"ok": False}), 400

    # Salvar no Disco Local (Temp)
    safe_name = secure_filename(file.filename)
    temp_dir = os.path.join(current_app.root_path, '..', 'storage', 'queue')
    os.makedirs(temp_dir, exist_ok=True)

    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
    save_path = os.path.join(temp_dir, unique_name)
    file.save(save_path)
    file_size = os.path.getsize(save_path)

    # --- LÓGICA DE CORREÇÃO DO CHECKBOX ---
    if preserve_structure:
        # Se marcado: Usa o caminho completo vindo do JS (ex: PASTA/IMG/foto.jpg)
        final_drive_path = raw_relative_path 
    else:
        # Se desmarcado: Ignora qualquer pasta e usa apenas o nome do arquivo
        # Isso faz com que o upload_single_file_sync jogue o arquivo direto na raiz
        final_drive_path = file.filename 

    user_email = AuditService.get_current_user_email()

    new_history = UploadHistoryModel(
        task_id=task_id,
        filename=file.filename,
        relative_path=final_drive_path, # <--- Caminho corrigido aqui
        mime_type=file.mimetype,
        status='PENDING',
        destination_id=target_root_id,
        temp_path=save_path,
        size_bytes=file_size,
        user_email=user_email
    )
    
    db.session.add(new_history)
    db.session.commit()

    QueueService.start_worker(current_app._get_current_object())

    return jsonify({"ok": True, "file_id": new_history.id})

@upload_bp.route("/api/upload/batch-status", methods=["POST"])
def api_batch_status():
    data = request.json or {}
    task_ids = data.get('task_ids', [])

    if not task_ids:
        return jsonify({"percent": 0, "total_files": 0, "processed": 0, "success": 0, "error": 0})

    # Pegamos o ID do Lote (assumindo que mandamos apenas 1 ID no array, que é o ID do pai)
    batch_id = task_ids[0]
    
    task = db.session.get(TaskModel, batch_id)
    if not task:
        return jsonify({"percent": 0, "total_files": 0, "processed": 0})

    # Leitura direta da Task Pai
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
        "speed_mb_s": 0.0, # Implementar calc de velocidade depois se necessário
        "bytes_total": task.bytes_found,
        "bytes_downloaded": task.bytes_downloaded
    })