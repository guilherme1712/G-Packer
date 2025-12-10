# app/blueprints/upload.py
import os
from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from app.services.auth import get_credentials
from app.services.Google.drive_upload import DriveUploadService
from app.services.audit import AuditService

upload_bp = Blueprint("upload", __name__)

@upload_bp.route("/upload")
def index():
    """Renderiza a tela de upload."""
    creds = get_credentials()
    if not creds:
        return redirect(url_for('auth.login'))
    return render_template("upload.html")

@upload_bp.route("/api/upload", methods=["POST"])
def api_process_upload():
    """Processa o upload dos arquivos enviados."""
    creds = get_credentials()
    if not creds: return jsonify({"ok": False, "error": "Sessão expirada"}), 401
    
    # Destino padrão: root
    root_target_id = request.form.get("target_folder_id") or "root"
    
    uploaded_files = request.files.getlist("files")
    relative_paths = request.form.getlist("paths")
    
    if not uploaded_files:
        return jsonify({"ok": False, "error": "Nenhum arquivo enviado."}), 400
    
    # Se não vieram caminhos (upload simples), usa o nome do arquivo
    if len(relative_paths) != len(uploaded_files):
        relative_paths = [f.filename for f in uploaded_files]

    success_count = 0
    errors = []
    folder_cache = {} 

    for file, rel_path in zip(uploaded_files, relative_paths):
        if file.filename == '': continue
            
        try:
            # Normaliza caminho
            clean_path = rel_path.replace('\\', '/').strip('/')
            parts = clean_path.split('/')
            
            final_parent_id = root_target_id
            filename = file.filename

            # Se for estrutura de pasta (tem barras no caminho)
            if len(parts) > 1:
                folder_structure = parts[:-1]
                # Garante que as pastas existam
                final_parent_id = DriveUploadService.ensure_folder_path(
                    creds, root_target_id, folder_structure, folder_cache
                )
                filename = parts[-1]

            # Upload
            result = DriveUploadService.upload_file(
                creds, file.stream, filename, 
                parent_id=final_parent_id, mimetype=file.mimetype
            )
            
            size_bytes = result.get('size', '0')
            AuditService.log("UPLOAD", clean_path, details={"size": size_bytes, "id": result.get('id')})
            success_count += 1
            
        except Exception as e:
            errors.append(f"{rel_path}: {str(e)}")
            AuditService.log("UPLOAD_ERROR", rel_path, details=str(e))

    return jsonify({
        "ok": True, "count": success_count, "errors": errors,
        "message": f"{success_count} itens enviados com sucesso."
    })