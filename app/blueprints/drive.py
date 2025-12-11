# controllers/drive_controller.py
import os
import time
import json
import shutil
from threading import Thread
from datetime import datetime  # não precisamos mais de timedelta aqui

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

from app.services.auth import get_credentials
from app.services.Google.drive_filters import build_filters_from_form
from app.services.Google.drive_tree import (
    get_file_metadata,
    calculate_selection_stats,
    get_ancestors_path,
)
from app.services.Google.drive_cache import (
    get_children_cached,
    rebuild_full_cache,
    search_cache,
)
from app.services.audit import AuditService
from app.services.Google.drive_download import download_items_bundle, mirror_items_to_local
from app.services.Google.drive_activity import fetch_activity_log
from app.services.storage import StorageService
from app.services.Google.drive_ops import DriveOperationsService
from app.services.progress import (
    PROGRESS,
    init_download_task,
    get_task_progress,
    sync_task_to_db,
    get_all_active_tasks,
    set_task_pause,
    set_task_cancel,
)

from app.models import db, FavoriteModel
from app.models.backup_file import BackupFileModel, apply_global_retention  # <<< AQUI

drive_bp = Blueprint("drive", __name__)

BACKUP_FOLDER_NAME = "storage/backups"


def _parse_positive_int(value):
    """
    Converte um valor (string/int/None) em inteiro positivo.
    Retorna None se estiver vazio, inválido ou <= 0.
    """
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        v = int(value)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _apply_retention_policy(storage_root_path: str | None = None):
    """
    Aplica a política de retenção global usando os valores de config:

        BACKUP_RETENTION_MAX_FILES (quantidade)
        BACKUP_RETENTION_MAX_DAYS  (idade em dias)

    OBS: o parâmetro storage_root_path é mantido só por compatibilidade,
    a remoção é feita usando o campo .path de cada BackupFileModel.
    """
    max_backups = _parse_positive_int(
        current_app.config.get("BACKUP_RETENTION_MAX_FILES")
    )
    max_days = _parse_positive_int(
        current_app.config.get("BACKUP_RETENTION_MAX_DAYS")
    )

    # Se nada estiver configurado, não faz nada
    if not max_backups and not max_days:
        return

    try:
        apply_global_retention(max_backups=max_backups, max_days=max_days)
    except Exception as e:
        try:
            current_app.logger.warning(
                f"Erro ao aplicar política global de retenção de backups: {e}"
            )
        except Exception:
            print(f"Erro ao aplicar política global de retenção de backups: {e}")



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
    backup_type="full",  # 'full' ou 'incremental'
    client_ip=None,   # [NOVO]
    user_email=None   # [NOVO]
):
    with app_context:
        try:
            if output_mode == "mirror":
                # Modo "espelho" (mirror) - não gera arquivo de backup .zip,
                # apenas baixa os arquivos para uma pasta local.
                if not local_mirror_path:
                    raise Exception("Caminho local inválido")

                mirror_items_to_local(
                    creds,
                    items,
                    dest_root=local_mirror_path,
                    progress_dict=PROGRESS,
                    task_id=task_id,
                    filters=filters,
                    processing_mode=processing_mode,
                )
                # Mirror não gera registro de BackupFileModel snapshot por enquanto
            else:
                # Modo Archive (ZIP/TAR) com suporte a FULL/INCREMENTAL
                # download_items_bundle agora retorna (path, current_manifest)
                 # 1. Determina a Série ANTES de processar
                 # Precisamos disso para achar o manifesto anterior se for incremental
                try:
                    series_key = BackupFileModel.build_series_key(zip_file_name, items)
                    if not series_key:
                        series_key = BackupFileModel._normalize_series_key(None, origin_task_id=task_id)
                except:
                    series_key = BackupFileModel._normalize_series_key(None, origin_task_id=task_id)

                last_snapshot = BackupFileModel.get_last_snapshot(series_key)

                # Se pediu incremental mas não tem pai, vira Full forçado
                if backup_type == "incremental" and not last_snapshot:
                    print(f"Task {task_id}: Incremental solicitado, mas sem backup anterior. Forçando FULL.")
                    backup_type = "full"

                previous_manifest = None
                if backup_type == "incremental" and last_snapshot and last_snapshot.metadata_json:
                    previous_manifest = last_snapshot.metadata_json.get("manifest")

                result = download_items_bundle(
                    creds,
                    items,
                    base_name=zip_file_name,
                    compression_level=compression_level,
                    archive_format=archive_format,
                    progress_dict=PROGRESS,
                    task_id=task_id,
                    filters=filters,
                    processing_mode=processing_mode,
                    previous_manifest=previous_manifest
                )

                # Desempacota retorno (suporta versão antiga que retornava só string)
                if isinstance(result, tuple):
                    temp_zip_path, current_manifest = result
                else:
                    temp_zip_path, current_manifest = result, {}

                # -----------------------------------------
                # Define próxima versão
                # -----------------------------------------
                next_version = ((last_snapshot.version_index or 0) + 1) if last_snapshot else 1

                # Define se é Full ou Inc baseado no que de fato ocorreu
                # (Se não passou previous_manifest, foi Full)
                is_full = (previous_manifest is None)

                # Gera nome físico
                original_name = os.path.basename(temp_zip_path)
                lower = original_name.lower()
                if lower.endswith(".tar.gz"):
                    base, ext = original_name[:-7], ".tar.gz"
                else:
                    base, ext = os.path.splitext(original_name)

                # Sufixo do arquivo
                type_tag = "INC" if not is_full else "FULL"
                task_suffix = str(task_id).replace(":", "")[-6:]
                versioned_name = f"{base}_v{next_version}_{type_tag}_{task_suffix}{ext}"
                final_dest_path = os.path.join(storage_root_path, versioned_name)

                shutil.move(temp_zip_path, final_dest_path)

                PROGRESS[task_id]["final_filename"] = versioned_name
                PROGRESS[task_id]["message"] = "Arquivo salvo com sucesso."

                # -----------------------------------------
                # Registro no Banco
                # -----------------------------------------
                try:
                    stat_info = os.stat(final_dest_path)
                    size_mb = round(stat_info.st_size / (1024 * 1024), 2)

                    # Se foi incremental, items_count é quantos baixou no ZIP
                    # Mas o 'manifest' guardamos O ESTADO COMPLETO da pasta para o próximo diff
                    items_downloaded = PROGRESS.get(task_id, {}).get("files_downloaded", 0)

                    parent_id = last_snapshot.id if (not is_full and last_snapshot) else None

                    metadata = {
                        "archive_format": archive_format,
                        "filters": filters or {},
                        "processing_mode": processing_mode,
                        "manifest": current_manifest  # Guarda estado atual completo
                    }

                    BackupFileModel.register_snapshot(
                        filename=versioned_name,
                        path=final_dest_path,
                        size_mb=size_mb,
                        items_count=items_downloaded,
                        origin_task_id=task_id,
                        series_key=series_key,
                        is_full=is_full,
                        parent_id=parent_id,
                        metadata=metadata,
                    )

                    _apply_retention_policy(storage_root_path)

                    final_target = local_mirror_path if output_mode == "mirror" else versioned_name
                    final_size = "N/A"

                    if output_mode != "mirror":
                        try:
                            stat_info = os.stat(final_dest_path)
                            final_size = f"{round(stat_info.st_size / (1024 * 1024), 2)} MB"
                        except: pass

                    details_json = json.dumps({
                        "size_mb": size_mb,
                        "items": items_downloaded,
                        "type": type_tag,
                        "mode": output_mode
                    })

                    AuditService.log(
                        action_type="DOWNLOAD_COMPLETE",
                        target=versioned_name,
                        ip_address=client_ip,
                        user_email=user_email,
                        details=details_json
                    )

                except Exception as db_err:
                    db.session.rollback()
                    print(f"Erro DB snapshot: {db_err}")
                    PROGRESS[task_id]["phase"] = "erro"
                    PROGRESS[task_id]["message"] = f"Erro ao registrar: {db_err}"

            sync_task_to_db(task_id)

        except Exception as e:
            PROGRESS[task_id]["phase"] = "erro"
            PROGRESS[task_id]["message"] = f"Falha: {e}"

            AuditService.log(
                action_type="DOWNLOAD_ERROR",
                target=zip_file_name,
                ip_address=client_ip,
                user_email=user_email,
                details=str(e)
            )

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
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    include_files = request.args.get("files") == "1"
    force = request.args.get("force") == "1"

    items = get_children_cached(
        creds,
        "root",
        include_files=include_files,
        force_refresh=force,
    )
    return jsonify({"items": items})


@drive_bp.route("/api/folders/children/<folder_id>")
def api_folders_children(folder_id):
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    include_files = request.args.get("files") == "1"
    force = request.args.get("force") == "1"

    items = get_children_cached(
        creds,
        folder_id,
        include_files=include_files,
        force_refresh=force,
    )
    return jsonify({"items": items})

@drive_bp.route("/api/cache/rebuild", methods=["POST"])
def api_drive_cache_rebuild():
    """
    Recria o cache local do Drive a partir de 'root'.

    Body (JSON) opcional:
      {
        "include_files": true,   # default true
      }
    """
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    include_files = bool(data.get("include_files", True))

    try:
        total = rebuild_full_cache(creds, include_files=include_files)
        return jsonify({"ok": True, "total_items": total})
    except Exception as e:
        current_app.logger.exception("Erro ao reconstruir cache do Drive")
        return jsonify({"ok": False, "error": str(e)}), 500

@drive_bp.route("/api/cache/search")
def api_drive_cache_search():
    """
    Busca em cima do cache local do Drive.

    Query params:
      q        = texto no nome (opcional)
      type     = "file" | "folder" (opcional)
      min_size = mínimo em bytes (opcional)
      max_size = máximo em bytes (opcional)
      limit    = máximo de registros (default 200)
    """
    # Eu exigiria login, mas não preciso do creds em si
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    def _parse_int(value):
        try:
            return int(value) if value not in (None, "", "null") else None
        except Exception:
            return None

    text = request.args.get("q") or None
    type_filter = request.args.get("type") or None
    min_size = _parse_int(request.args.get("min_size"))
    max_size = _parse_int(request.args.get("max_size"))
    limit = _parse_int(request.args.get("limit")) or 200

    results = search_cache(
        text=text,
        type_filter=type_filter,
        min_size=min_size,
        max_size=max_size,
        limit=limit,
    )
    return jsonify({"ok": True, "results": results})


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
        AuditService.log("FAVORITE_ADD", data.get('name'), details=data.get('path'))

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

    # ... (Lógica de items e zip_name mantida igual) ...
    items_raw = data.get("items_json")
    if isinstance(items_raw, str):
        try: items = json.loads(items_raw)
        except: items = []
    else: items = items_raw or []

    if not items: return jsonify({"ok": False, "error": "Nenhum item selecionado"}), 400

    zip_name = (data.get("zip_name") or "backup").strip()
    if zip_name.lower().endswith(".zip"): zip_name = zip_name[:-4]

    archive_format = data.get("archive_format") or "zip"
    compression_level = data.get("compression_level") or "normal"
    output_mode = data.get("output_mode") or "archive"
    local_mirror_path = (data.get("local_mirror_path") or "").strip()
    processing_mode = data.get("processing_mode") or "sequential"

    # --- CORREÇÃO AQUI: Captura a escolha do usuário ---
    backup_type = data.get("backup_type", "full")

    storage_root_path = StorageService.backups_dir()
    task_id = data.get("task_id") or f"task-{int(time.time())}"
    init_download_task(task_id)

    client_ip = request.remote_addr
    user_email = AuditService.get_current_user_email()

    thread = Thread(
        target=_background_backup_task,
        args=(
            current_app.app_context(),
            task_id,
            creds,
            items,
            output_mode,
            local_mirror_path,
            storage_root_path,
            zip_name,
            compression_level,
            archive_format,
            build_filters_from_form(data),
            processing_mode,
            backup_type,
            client_ip,
            user_email
        ),
        daemon=True,
    )
    thread.start()

    return jsonify({"ok": True, "task_id": task_id})



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
    storage_path = StorageService.backups_dir()
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

@drive_bp.route("/api/analyze", methods=["POST"])
def analyze_selection():
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Sessão expirada"}), 401

    data = request.get_json() or {}
    items = data.get("items", [])

    if not items:
        return jsonify({"ok": False, "error": "Nenhum item"}), 400

    try:
        stats = calculate_selection_stats(creds, items)

        # Formata tamanho para humano
        size_bytes = stats["total_size_bytes"]
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                stats["size_formatted"] = f"{size_bytes:.2f} {unit}"
                break
            size_bytes /= 1024

        return jsonify({"ok": True, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# -------------------------------------------------------------------------
#  NOVO ENDPOINT: CHECAR SE JÁ EXISTE SÉRIE (Para perguntar Full/Inc)
# -------------------------------------------------------------------------
@drive_bp.route("/api/backup/check-series", methods=["POST"])
def check_backup_series():
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Sessão expirada"}), 401

    data = request.get_json() or {}
    items = data.get("items") or []
    zip_name = data.get("zip_name", "backup")

    # Calcula a chave da série
    series_key = BackupFileModel.build_series_key(zip_name, items)

    # Verifica se já existe algum backup dessa série
    last = BackupFileModel.get_last_snapshot(series_key)

    if last:
        return jsonify({
            "ok": True,
            "exists": True,
            "series_key": series_key,
            "last_version": last.version_index,
            "last_date": last.created_at.isoformat() if last.created_at else None,
            "is_full": last.is_full
        })
    else:
        return jsonify({
            "ok": True,
            "exists": False,
            "series_key": series_key
        })

@drive_bp.route("/api/file/rename", methods=["POST"])
def api_rename():
    creds = get_credentials()
    if not creds: return jsonify({"ok": False, "error": "Auth required"}), 401
    
    data = request.json or {}
    result = DriveOperationsService.rename_file(creds, data.get('id'), data.get('name'))
    return jsonify(result)

@drive_bp.route("/api/file/move", methods=["POST"])
def api_move():
    creds = get_credentials()
    if not creds: return jsonify({"ok": False, "error": "Auth required"}), 401
    
    data = request.json or {}
    # Aceita drag & drop: file_id (item) -> target_id (pasta destino)
    result = DriveOperationsService.move_file(creds, data.get('file_id'), data.get('target_id'))
    return jsonify(result)

@drive_bp.route("/api/file/trash", methods=["POST"])
def api_trash():
    creds = get_credentials()
    if not creds: return jsonify({"ok": False, "error": "Auth required"}), 401
    
    data = request.json or {}
    result = DriveOperationsService.trash_file(creds, data.get('id'))
    return jsonify(result)

@drive_bp.route("/api/folder/create", methods=["POST"])
def api_create_folder():
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Auth required"}), 401

    data = request.json or {}
    parent_id = data.get("parent_id")
    name = data.get("name")

    if not parent_id or not name:
        return jsonify({"ok": False, "error": "Missing parent_id or name"}), 400

    result = DriveOperationsService.create_folder(creds, parent_id, name)
    return jsonify(result)
