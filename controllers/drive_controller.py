# controllers/drive_controller.py
import os
import time
import json
import shutil
from threading import Thread
from datetime import datetime

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
    send_from_directory,
    flash,
    current_app,
)

from services.auth_service import get_credentials
from services.drive_filters import build_filters_from_form
from services.drive_tree_service import get_children, get_file_metadata, get_ancestors_path
from services.drive_download_service import download_items_bundle, mirror_items_to_local
from services.drive_activity_service import fetch_activity_log
from services.progress_service import (
    PROGRESS,
    init_download_task,
    get_task_progress,
    sync_task_to_db,
    get_all_active_tasks,
    set_task_pause,
    set_task_cancel
)
from models import db, BackupFileModel, FavoriteModel

drive_bp = Blueprint("drive", __name__)

BACKUP_FOLDER_NAME = "storage/backups"


def _background_backup_task(
    app_context,
    task_id,
    creds,
    items,
    output_mode,
    local_mirror_path,
    storage_root_path,
    zip_file_name,
    compression_level,
    archive_format,
    filters,
    processing_mode,
):
    with app_context:
        try:
            if output_mode == "mirror":
                if not local_mirror_path:
                    raise Exception("Caminho local inválido")

                mirror_items_to_local(
                    creds,
                    items,
                    dest_root=local_mirror_path,
                    progress_dict=PROGRESS,
                    task_id=task_id,
                    filters=filters,
                    processing_mode=processing_mode
                )
            else:
                temp_zip_path = download_items_bundle(
                    creds,
                    items,
                    base_name=zip_file_name,
                    compression_level=compression_level,
                    archive_format=archive_format,
                    progress_dict=PROGRESS,
                    task_id=task_id,
                    filters=filters,
                    processing_mode=processing_mode
                )

                generated_filename = os.path.basename(temp_zip_path)
                final_dest_path = os.path.join(storage_root_path, generated_filename)

                shutil.move(temp_zip_path, final_dest_path)

                PROGRESS[task_id]["final_filename"] = generated_filename
                PROGRESS[task_id]["message"] = "Arquivo gerado e salvo com sucesso."

                try:
                    stat = os.stat(final_dest_path)
                    size_mb = round(stat.st_size / (1024 * 1024), 2)
                    items_count = PROGRESS.get(task_id, {}).get("files_total", 0)

                    existing = BackupFileModel.query.filter_by(
                        filename=generated_filename
                    ).first()
                    if not existing:
                        bf = BackupFileModel(
                            filename=generated_filename,
                            path=final_dest_path,
                            size_mb=size_mb,
                            items_count=items_count,
                            origin_task_id=task_id,
                        )
                        db.session.add(bf)
                    else:
                        existing.path = final_dest_path
                        existing.size_mb = size_mb
                        existing.items_count = items_count
                        existing.origin_task_id = task_id
                        existing.created_at = datetime.utcnow()

                    db.session.commit()
                except Exception as db_err:
                    db.session.rollback()
                    print(f"Erro ao salvar BackupFileModel: {db_err}")

                sync_task_to_db(task_id)

        except Exception as e:
            print(f"Erro na thread de backup {task_id}: {e}")
            PROGRESS.setdefault(task_id, {})
            PROGRESS[task_id]["phase"] = "erro" if "Cancelado" not in str(e) else "cancelado"
            PROGRESS[task_id]["message"] = str(e)
            hist = PROGRESS[task_id].get("history", [])
            hist.append(f"Fim: {str(e)}")
            PROGRESS[task_id]["history"] = hist
            sync_task_to_db(task_id)


@drive_bp.route("/folders")
def folders():
    creds = get_credentials()
    if not creds:
        flash("Faça login no Google primeiro.")
        return redirect(url_for("auth.index"))

    return render_template("folders.html")


@drive_bp.route("/api/folders/root")
def api_folders_root():
    creds = get_credentials()
    if not creds: return jsonify({"error": "unauthorized"}), 401
    include_files = request.args.get("files") == "1"
    items = get_children(creds, "root", include_files=include_files)
    return jsonify({"items": items})


@drive_bp.route("/api/folders/children/<folder_id>")
def api_folders_children(folder_id):
    creds = get_credentials()
    if not creds: return jsonify({"error": "unauthorized"}), 401
    include_files = request.args.get("files") == "1"
    items = get_children(creds, folder_id, include_files=include_files)
    return jsonify({"items": items})


@drive_bp.route("/api/path/<file_id>")
def api_resolve_path(file_id):
    creds = get_credentials()
    if not creds: return jsonify({"error": "unauthorized"}), 401
    path_ids = get_ancestors_path(creds, file_id)
    return jsonify({"path": path_ids})


@drive_bp.route("/api/file/<file_id>")
def api_file_details(file_id):
    creds = get_credentials()
    if not creds: return jsonify({"error": "unauthorized"}), 401
    meta = get_file_metadata(creds, file_id)
    return jsonify(meta)


@drive_bp.route("/api/activity/<file_id>")
def api_file_activity(file_id):
    creds = get_credentials()
    if not creds: return jsonify({"error": "unauthorized"}), 401

    activities = fetch_activity_log(creds, file_id)
    return jsonify({"activity": activities})


@drive_bp.route("/api/favorites", methods=["GET"])
def list_favorites():
    favs = FavoriteModel.query.all()
    return jsonify({"favorites": [f.to_dict() for f in favs]})


@drive_bp.route("/api/favorites", methods=["POST"])
def add_favorite():
    data = request.json or {}
    item_id = data.get('id')
    name = data.get('name')
    path = data.get('path')
    item_type = data.get('type', 'folder')

    if not item_id or not name:
        return jsonify({"ok": False, "error": "Dados inválidos"}), 400

    existing = FavoriteModel.query.get(item_id)
    if existing:
        existing.name = name
        existing.path = path
        existing.type = item_type
    else:
        new_fav = FavoriteModel(id=item_id, name=name, path=path, type=item_type)
        db.session.add(new_fav)


    try:
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@drive_bp.route("/api/favorites/<item_id>", methods=["DELETE"])
def delete_favorite(item_id):
    fav = FavoriteModel.query.get(item_id)
    if fav:
        try:
            db.session.delete(fav)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True})


@drive_bp.route("/download", methods=["POST"])
def download():
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Sessão expirada"}), 401

    data = request.get_json() or {}
    items_raw = data.get("items_json")
    if isinstance(items_raw, str):
        try: items = json.loads(items_raw)
        except: items = []
    else:
        items = items_raw or []

    if not items:
        return jsonify({"ok": False, "error": "Nenhum item selecionado"}), 400

    zip_name = (data.get("zip_name") or "backup").strip()
    if zip_name.lower().endswith(".zip"): zip_name = zip_name[:-4]

    archive_format = data.get("archive_format") or "zip"
    compression_level = data.get("compression_level") or "normal"
    output_mode = data.get("output_mode") or "archive"
    local_mirror_path = (data.get("local_mirror_path") or "").strip()

    execution_mode = data.get("execution_mode") or "immediate"
    
    # Aqui pegamos o modo (concurrent/sequential) com fallback para sequential
    processing_mode = data.get("processing_mode") or "sequential"

    task_id = data.get("task_id") or f"task-{int(time.time())}"
    filters = build_filters_from_form(data)

    storage_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(storage_path, exist_ok=True)

    init_download_task(task_id)
    PROGRESS[task_id]["history"] = []
    PROGRESS[task_id]["canceled"] = False
    PROGRESS[task_id]["output_mode"] = output_mode
    PROGRESS[task_id]["final_filename"] = None
    sync_task_to_db(task_id)

    app_ctx = current_app.app_context()
    t = Thread(
        target=_background_backup_task,
        args=(app_ctx, task_id, creds, items, output_mode, local_mirror_path,
              storage_path, zip_name, compression_level, archive_format, filters, processing_mode),
    )
    t.start()

    return jsonify({
        "ok": True,
        "task_id": task_id,
        "message": "Processo iniciado",
        "mode": execution_mode
    })


@drive_bp.route("/progress/<task_id>")
def progress(task_id):
    data = get_task_progress(task_id)
    if data.get("phase") == "concluido":
        filename = PROGRESS.get(task_id, {}).get("final_filename")
        if filename:
            data["download_url"] = url_for("drive.get_file", filename=filename)
            data["filename"] = filename
    return jsonify(data)


@drive_bp.route("/drive/get-file/<path:filename>")
def get_file(filename):
    creds = get_credentials()
    if not creds:
        flash("Sessão expirada.")
        return redirect(url_for("auth.index"))
    storage_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    return send_from_directory(storage_path, filename, as_attachment=True)


@drive_bp.route("/cancel/<task_id>", methods=["POST"])
def cancel_task(task_id):
    set_task_cancel(task_id)
    return jsonify({"ok": True})


@drive_bp.route("/api/tasks/active")
def api_active_tasks():
    return jsonify({"tasks": get_all_active_tasks()})

@drive_bp.route("/api/tasks/pause/<task_id>", methods=["POST"])
def api_pause_task(task_id):
    set_task_pause(task_id, True)
    return jsonify({"ok": True})

@drive_bp.route("/api/tasks/resume/<task_id>", methods=["POST"])
def api_resume_task(task_id):
    set_task_pause(task_id, False)
    return jsonify({"ok": True})