import os
import shutil
import uuid
import time
from flask import Blueprint, render_template, request, jsonify, send_file, url_for, current_app, redirect
from werkzeug.utils import secure_filename

from app.models import db, TaskModel, BackupFileModel
from app.enum.task_type import TaskTypeEnum
from app.services.storage import StorageService
from app.services.worker_manager import WorkerManager
from app.services.tools.packer_service import PackerService
from app.services.auth import get_credentials
from app.services.google.drive_upload import DriveUploadService

packer_bp = Blueprint("packer", __name__)


def _get_temp_pack_dir(job_id):
    path = os.path.join(StorageService.temp_work_dir(), f"pack_{job_id}")
    StorageService.ensure_dir(path)
    return path


# --- PÁGINA 1: UPLOAD ---
@packer_bp.route("/packer", methods=["GET"])
def index():
    return render_template("tools/packer_upload.html")


# --- PÁGINA 2: RESULTADO/PROGRESSO ---
@packer_bp.route("/packer/job/<job_id>", methods=["GET"])
def job_view(job_id):
    """Renderiza a página de resultado para um Job específico."""
    task = TaskModel.query.get(job_id)
    if not task:
        return render_template("tools/packer_upload.html", error="Tarefa não encontrada ou expirada.")

    return render_template("tools/packer_result.html", job_id=job_id)


@packer_bp.route("/packer/upload", methods=["POST"])
def upload_start_task():
    try:
        files = request.files.getlist("files")
        if not files or files[0].filename == '':
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado."}), 400

        job_id = uuid.uuid4().hex[:8]
        work_dir = _get_temp_pack_dir(job_id)
        input_dir = os.path.join(work_dir, "input")
        StorageService.ensure_dir(input_dir)

        # 1. Salvar arquivos
        total_size = 0
        file_count = 0
        for file in files:
            filename = secure_filename(file.filename)
            rel_path = file.filename

            # Tratamento simples de caminhos relativos
            if "/" in rel_path or "\\" in rel_path:
                safe_parts = [secure_filename(p) for p in rel_path.replace("\\", "/").split("/")]
                save_path = os.path.join(input_dir, *safe_parts)
                StorageService.ensure_parent_dir(save_path)
            else:
                save_path = os.path.join(input_dir, filename)

            file.save(save_path)
            total_size += os.path.getsize(save_path)
            file_count += 1

        # 2. Configurações
        zip_name = request.form.get("zip_name", "pacote").strip() or "pacote"
        fmt = request.form.get("format", "zip")
        level = request.form.get("level", "normal")
        output_filename = f"{zip_name}.{fmt}"
        output_path = os.path.join(work_dir, output_filename)

        # 3. Cria Task
        new_task = TaskModel(
            id=job_id,
            type=TaskTypeEnum.COMPRESSION,
            phase="PENDING",
            message="Aguardando fila...",
            files_total=file_count,
            bytes_found=total_size,
            files_downloaded=0,
            bytes_downloaded=0
        )
        db.session.add(new_task)
        db.session.commit()

        # 4. Envia para Worker
        executor = WorkerManager.get_compression_executor()
        app_obj = current_app._get_current_object()

        executor.submit(
            PackerService.run_compression_task,
            app_obj,
            job_id,
            input_dir,
            output_path,
            fmt,
            level
        )

        # Retorna a URL de redirecionamento para a página de resultado
        return jsonify({
            "ok": True,
            "redirect_url": url_for("packer.job_view", job_id=job_id)
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@packer_bp.route("/packer/status/<job_id>", methods=["GET"])
def check_status(job_id):
    task = TaskModel.query.get(job_id)
    if not task:
        return jsonify({"ok": False, "phase": "NOT_FOUND"}), 404

    task_data = task.to_dict()

    response = {
        "ok": True,
        "phase": task.phase,
        "message": task.message,
        "percent": task_data.get("percent", 0),
        "filename": f"pacote_{job_id}.zip"  # Nome provisório se ainda não terminou
    }

    # Tenta descobrir o nome real do arquivo se a task já tiver terminado
    # (Poderíamos salvar o output_filename no TaskModel para facilitar, mas vamos inferir ou passar via API)
    # Aqui, para simplificar, o front vai usar o nome que o status retornar ou genérico

    if task.phase == "COMPLETED":
        size_mb = round((task.bytes_downloaded or 0) / (1024 * 1024), 2)
        response["size_mb"] = size_mb

    return jsonify(response)


@packer_bp.route("/packer/action", methods=["POST"])
def execute_action():
    data = request.json or {}
    action = data.get("action")
    job_id = data.get("job_id")
    # O filename vem do frontend, que deve pegar do contexto ou status
    # Como agora estamos em outra página, precisamos garantir que o filename seja recuperado corretamente.
    # Vou assumir que o frontend passa o filename que ele descobriu no status ou input.
    # Mas para segurança, vamos tentar re-descobrir o arquivo na pasta temp.

    work_dir = _get_temp_pack_dir(job_id)

    # Procura o arquivo gerado na pasta (já que o nome pode variar)
    # Isso é mais seguro do que confiar no JS
    try:
        files_in_dir = [f for f in os.listdir(work_dir) if os.path.isfile(os.path.join(work_dir, f))]
        if not files_in_dir:
            return jsonify({"ok": False, "error": "Arquivo não encontrado. Talvez tenha expirado."}), 404

        # Pega o primeiro arquivo (o zip gerado)
        filename = files_in_dir[0]
        file_path = os.path.join(work_dir, filename)
    except Exception as e:
        return jsonify({"ok": False, "error": "Erro ao localizar arquivo."}), 500

    try:
        if action == "save_local":
            dest_dir = StorageService.backups_dir()
            dest_path = os.path.join(dest_dir, filename)
            if os.path.exists(dest_path):
                base, ext = os.path.splitext(filename)
                dest_path = os.path.join(dest_dir, f"{base}_{int(time.time())}{ext}")

            shutil.copy2(file_path, dest_path)

            size_mb = os.path.getsize(dest_path) / (1024 * 1024)
            bf = BackupFileModel(
                filename=os.path.basename(dest_path),
                path=dest_path,
                size_mb=size_mb,
                items_count=1,
                origin_task_id=f"PACKER_{job_id}",
                series_key="packer:manual"
            )
            db.session.add(bf)
            db.session.commit()

            shutil.rmtree(work_dir, ignore_errors=True)
            return jsonify({"ok": True, "message": "Salvo em Meus Backups."})

        elif action == "upload_drive":
            creds = get_credentials()
            if not creds: return jsonify({"ok": False, "error": "Login Google necessário."}), 401

            mime = "application/zip"
            if filename.endswith(".7z"):
                mime = "application/x-7z-compressed"
            elif "tar" in filename:
                mime = "application/gzip"

            DriveUploadService.upload_single_file_sync(
                creds, file_path, filename, mimetype=mime,
                relative_path=None, root_target_id="root",
                user_email="sistema"
            )
            shutil.rmtree(work_dir, ignore_errors=True)
            return jsonify({"ok": True, "message": "Enviado para o Drive."})

        elif action == "download_pc":
            return jsonify({
                "ok": True,
                "redirect_url": url_for("packer.download_temp", job_id=job_id, filename=filename)
            })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": False, "error": "Ação inválida"}), 400


@packer_bp.route("/packer/download/<job_id>/<filename>")
def download_temp(job_id, filename):
    work_dir = _get_temp_pack_dir(job_id)
    file_path = os.path.join(work_dir, filename)
    return send_file(file_path, as_attachment=True, download_name=filename)