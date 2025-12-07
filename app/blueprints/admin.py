# controllers/admin_controller.py
import os
import json
import io
import zipfile
import tarfile
import mimetypes


from datetime import datetime
from sqlalchemy import text, inspect, func

from flask import (
    Blueprint,
    render_template,
    jsonify,
    current_app,
    flash,
    redirect,
    url_for,
    request,
    send_file,
)

from app.services.auth import get_credentials
from app.models import (
    db,
    TaskModel,
    BackupProfileModel,
    BackupFileModel,
    ScheduledTaskModel,
    ScheduledRunModel,
    GoogleAuthModel,
)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

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
        if "tasks" in existing_tables:
            columns = [c["name"] for c in inspector.get_columns("tasks")]

            with db.engine.connect() as conn:
                if "paused" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE tasks "
                            "ADD COLUMN paused BOOLEAN DEFAULT 0"
                        )
                    )
                    conn.commit()

                if "canceled" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE tasks "
                            "ADD COLUMN canceled BOOLEAN DEFAULT 0"
                        )
                    )
                    conn.commit()

        # --- Migração Tabela BACKUP_PROFILES ---
        if "backup_profiles" in existing_tables:
            columns = [c["name"] for c in inspector.get_columns("backup_profiles")]

            with db.engine.connect() as conn:
                if "zip_name" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE backup_profiles "
                            "ADD COLUMN zip_name VARCHAR(150)"
                        )
                    )
                    conn.commit()

                if "execution_mode" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE backup_profiles "
                            "ADD COLUMN execution_mode VARCHAR(20) "
                            "DEFAULT 'immediate'"
                        )
                    )
                    conn.commit()

                if "processing_mode" not in columns:
                    print(
                        "AUTOFIX: Adicionando coluna 'processing_mode' "
                        "em 'backup_profiles'..."
                    )
                    conn.execute(
                        text(
                            "ALTER TABLE backup_profiles "
                            "ADD COLUMN processing_mode VARCHAR(20) "
                            "DEFAULT 'sequential'"
                        )
                    )
                    conn.commit()

                if "encrypt_zip" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE backup_profiles "
                            "ADD COLUMN encrypt_zip BOOLEAN DEFAULT 0"
                        )
                    )
                    conn.commit()

                if "zip_password" not in columns:
                    conn.execute(
                        text(
                            "ALTER TABLE backup_profiles "
                            "ADD COLUMN zip_password VARCHAR(255)"
                        )
                    )
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
    profiles = BackupProfileModel.query.all()

    sched_tasks = ScheduledTaskModel.query.order_by(
        ScheduledTaskModel.active.desc(),
        ScheduledTaskModel.next_run_at.asc(),
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
        GoogleAuthModel.query.filter_by(active=True)
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
    """
    Garante que todos os arquivos em storage/backups estejam refletidos
    na tabela backup_files.
    """
    folder_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(folder_path, exist_ok=True)

    existing_by_name = {b.filename: b for b in BackupFileModel.query.all()}
    changed = False

    for fname in os.listdir(folder_path):
        full_path = os.path.join(folder_path, fname)
        if not os.path.isfile(full_path):
            continue

        lower = fname.lower()
        if not (
            lower.endswith(".zip")
            or lower.endswith(".tar.gz")
            or lower.endswith(".tar")
        ):
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
            print(
                f"Erro ao sincronizar arquivo de backup '{fname}' para o DB: {e}"
            )

    if changed:
        try:
            db.session.commit()
        except Exception as e:
            print(f"Erro ao commit na sincronização de backups: {e}")
            db.session.rollback()


def _build_archive_tree(archive_path: str):
    """
    Lê o cabeçalho do ZIP/TAR e monta uma árvore leve (sem extrair arquivos).
    Retorna (nodes, files_count):

    nodes: lista de nós de topo, cada nó:
      {
        "name": "Documents",
        "path": "Documents",
        "type": "folder" | "file",
        "size": 1234,          # só em arquivos
        "children": [...]      # só em pastas
      }
    """
    root = {"name": "/", "path": "", "type": "folder", "children": {}}
    files_count = 0
    lower = archive_path.lower()

    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            for info in zf.infolist():
                name = info.filename or ""
                if not name:
                    continue

                is_dir = name.endswith("/") or getattr(info, "is_dir", lambda: False)()
                if name.endswith("/"):
                    name = name.rstrip("/")

                parts = [p for p in name.split("/") if p]
                if not parts:
                    continue

                node = root
                current_path = ""
                for i, part in enumerate(parts):
                    is_last = i == len(parts) - 1
                    current_path = f"{current_path}/{part}" if current_path else part
                    children = node["children"]

                    if is_last:
                        if is_dir:
                            children.setdefault(
                                part,
                                {
                                    "name": part,
                                    "path": current_path,
                                    "type": "folder",
                                    "children": {},
                                },
                            )
                        else:
                            files_count += 1
                            children[part] = {
                                "name": part,
                                "path": current_path,
                                "type": "file",
                                "size": getattr(info, "file_size", None),
                            }
                    else:
                        if part not in children:
                            children[part] = {
                                "name": part,
                                "path": current_path,
                                "type": "folder",
                                "children": {},
                            }
                        node = children[part]
    else:
        # TAR / TAR.GZ
        with tarfile.open(archive_path, "r:*") as tf:
            for member in tf.getmembers():
                name = member.name or ""
                if not name:
                    continue

                if name.endswith("/"):
                    name = name.rstrip("/")

                parts = [p for p in name.split("/") if p]
                if not parts:
                    continue

                node = root
                current_path = ""
                for i, part in enumerate(parts):
                    is_last = i == len(parts) - 1
                    current_path = f"{current_path}/{part}" if current_path else part
                    children = node["children"]

                    if is_last:
                        if member.isdir():
                            children.setdefault(
                                part,
                                {
                                    "name": part,
                                    "path": current_path,
                                    "type": "folder",
                                    "children": {},
                                },
                            )
                        else:
                            files_count += 1
                            children[part] = {
                                "name": part,
                                "path": current_path,
                                "type": "file",
                                "size": getattr(member, "size", None),
                            }
                    else:
                        if part not in children:
                            children[part] = {
                                "name": part,
                                "path": current_path,
                                "type": "folder",
                                "children": {},
                            }
                        node = children[part]

    # Converte children {nome: node} -> lista, ordenada
    def normalize(node):
        if node["type"] == "folder":
            children_dict = node.get("children", {})
            node["children"] = []
            for key in sorted(children_dict.keys(), key=lambda s: s.lower()):
                child = children_dict[key]
                normalize(child)
                node["children"].append(child)

    normalize(root)
    return root["children"], files_count


# ============================
# LISTAGEM / DOWNLOAD / DELETE
# ============================


@admin_bp.route("/admin/backups")
def list_backups():
    """
    Tela que lista todos os backups salvos em storage/backups.
    """
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
                "id": b.id,
                "name": b.filename,
                "path": b.path,
                "size_mb": round(b.size_mb or 0.0, 2),
                "created_at": b.created_at.strftime("%d/%m/%Y %H:%M:%S")
                if b.created_at
                else "",
                "items_count": b.items_count or 0,
                "origin_task_id": b.origin_task_id,
                "encrypted": getattr(b, "encrypted", False),
            }
        )

    return render_template("admin_backups.html", files=files)


@admin_bp.route("/admin/backups/<int:backup_id>/download")
def download_backup_file(backup_id):
    """
    Faz o download bruto do arquivo de backup (ZIP/TAR) do servidor.
    """
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    backup = BackupFileModel.query.get_or_404(backup_id)

    if not backup.path or not os.path.exists(backup.path):
        flash("Arquivo de backup não encontrado no disco.", "error")
        return redirect(url_for("admin.list_backups"))

    return send_file(
        backup.path,
        as_attachment=True,
        download_name=backup.filename,
    )


@admin_bp.route("/admin/backups/<int:backup_id>/delete")
def delete_backup(backup_id):
    """
    Remove o registro do backup no banco e apaga o arquivo físico.
    """
    creds = get_credentials()
    if not creds:
        return "Acesso negado.", 403

    backup = BackupFileModel.query.get_or_404(backup_id)

    # Tenta apagar o arquivo do disco
    if backup.path and os.path.exists(backup.path):
        try:
            os.remove(backup.path)
        except Exception as e:
            print(f"Erro ao remover arquivo de backup: {e}")
            flash("Não foi possível remover o arquivo físico, mas o registro foi limpo.", "warning")

    # Remove do banco
    try:
        db.session.delete(backup)
        db.session.commit()
        flash("Backup removido com sucesso.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao remover backup do banco: {e}", "error")

    return redirect(url_for("admin.list_backups"))


# ============================
# APIs de EXPLORAÇÃO / RESTORE
# ============================


@admin_bp.route("/admin/api/backups/<int:backup_id>/tree")
def api_backup_tree(backup_id):
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 403

    backup = BackupFileModel.query.get_or_404(backup_id)

    if not backup.path or not os.path.exists(backup.path):
        return jsonify(
            {"ok": False, "error": "Arquivo de backup não encontrado."}
        ), 404

    try:
        tree, files_count = _build_archive_tree(backup.path)

        # Atualiza items_count se estiver desatualizado
        if backup.items_count != files_count:
            backup.items_count = files_count
            db.session.commit()

        return jsonify(
            {
                "ok": True,
                "tree": tree,
                "files_count": files_count,
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.route(
    "/admin/api/backups/<int:backup_id>/download-partial", methods=["POST"]
)
def api_backup_download_partial(backup_id):
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Acesso negado."}), 403

    backup = BackupFileModel.query.get_or_404(backup_id)
    if not backup.path or not os.path.exists(backup.path):
        return jsonify(
            {"ok": False, "error": "Arquivo de backup não encontrado."}
        ), 404

    data = request.get_json(silent=True) or {}
    paths = data.get("paths") or []
    if not isinstance(paths, list) or not paths:
        return jsonify({"ok": False, "error": "Nenhum arquivo selecionado."}), 400

    buf = io.BytesIO()

    # Sempre gera um ZIP parcial, independente do formato original
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as out_zip:
        lower = backup.path.lower()

        if lower.endswith(".zip"):
            with zipfile.ZipFile(backup.path, "r") as src_zip:
                for rel_path in paths:
                    try:
                        info = src_zip.getinfo(rel_path)
                    except KeyError:
                        continue
                    if info.is_dir():
                        continue
                    with src_zip.open(info, "r") as src_f:
                        out_zip.writestr(rel_path, src_f.read())
        else:
            with tarfile.open(backup.path, "r:*") as src_tar:
                for rel_path in paths:
                    try:
                        member = src_tar.getmember(rel_path)
                    except KeyError:
                        continue
                    if member.isdir():
                        continue
                    src_f = src_tar.extractfile(member)
                    if not src_f:
                        continue
                    out_zip.writestr(rel_path, src_f.read())

    buf.seek(0)
    base_name, _ = os.path.splitext(backup.filename)
    download_name = f"{base_name}__parcial.zip"

    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=download_name,
    )


@admin_bp.route(
    "/admin/api/backups/<int:backup_id>/restore-drive", methods=["POST"]
)
def api_backup_restore_drive(backup_id):
    creds = get_credentials()
    if not creds:
        return jsonify(
            {
                "ok": False,
                "error": "Você precisa estar autenticado no Google para restaurar arquivos.",
            }
        ), 403

    backup = BackupFileModel.query.get_or_404(backup_id)
    if not backup.path or not os.path.exists(backup.path):
        return jsonify(
            {"ok": False, "error": "Arquivo de backup não encontrado."}
        ), 404

    data = request.get_json(silent=True) or {}
    paths = data.get("paths") or []
    if not isinstance(paths, list) or not paths:
        return jsonify({"ok": False, "error": "Nenhum arquivo selecionado."}), 400

    service = build("drive", "v3", credentials=creds)

    # --- pasta raiz "Restaurados" no My Drive ---
    def ensure_root_restore_folder():
        query = (
            "name = 'Restaurados' "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and 'root' in parents "
            "and trashed = false"
        )
        resp = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                pageSize=1,
            )
            .execute()
        )
        files = resp.get("files", [])
        if files:
            return files[0]["id"]

        meta = {
            "name": "Restaurados",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["root"],
        }
        created = service.files().create(body=meta, fields="id").execute()
        return created["id"]

    restore_root_id = ensure_root_restore_folder()

    # cache para subpastas já criadas
    folder_cache = {}

    def ensure_subfolder(parts):
        """
        Cria (se não existir) a estrutura dentro de 'Restaurados',
        reaproveitando subpastas pelo cache.
        """
        parent_id = restore_root_id
        current_key = ""

        for part in parts:
            if not part:
                continue
            current_key = f"{current_key}/{part}" if current_key else part

            if current_key in folder_cache:
                parent_id = folder_cache[current_key]
                continue

            query = (
                f"name = '{part}' "
                f"and mimeType = 'application/vnd.google-apps.folder' "
                f"and '{parent_id}' in parents "
                f"and trashed = false"
            )
            resp = (
                service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, name)",
                    pageSize=1,
                )
                .execute()
            )
            files = resp.get("files", [])
            if files:
                folder_id = files[0]["id"]
            else:
                meta = {
                    "name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                }
                created = (
                    service.files()
                    .create(body=meta, fields="id")
                    .execute()
                )
                folder_id = created["id"]

            folder_cache[current_key] = folder_id
            parent_id = folder_id

        return parent_id

    uploaded = []
    lower = backup.path.lower()

    # ----- FUNÇÃO AUXILIAR PARA MIMETYPE -----
    def guess_mime_type(filename: str) -> str:
        mime, _ = mimetypes.guess_type(filename)
        if not mime:
            mime = "application/octet-stream"
        return mime

    if lower.endswith(".zip"):
        with zipfile.ZipFile(backup.path, "r") as src_zip:
            for rel_path in paths:
                try:
                    info = src_zip.getinfo(rel_path)
                except KeyError:
                    continue
                if info.is_dir():
                    continue

                parts = [p for p in rel_path.split("/") if p]
                if not parts:
                    continue
                *folder_parts, filename = parts

                parent_id = ensure_subfolder(folder_parts)

                with src_zip.open(info, "r") as src_f:
                    data_bytes = src_f.read()

                mime_type = guess_mime_type(filename)

                media = MediaIoBaseUpload(
                    io.BytesIO(data_bytes),
                    mimetype=mime_type,
                    resumable=False,
                )
                file_meta = {
                    "name": filename,
                    "parents": [parent_id],
                }
                created = (
                    service.files()
                    .create(
                        body=file_meta,
                        media_body=media,
                        fields="id, name",
                    )
                    .execute()
                )
                uploaded.append({"path": rel_path, "drive_id": created["id"]})
    else:
        with tarfile.open(backup.path, "r:*") as src_tar:
            for rel_path in paths:
                try:
                    member = src_tar.getmember(rel_path)
                except KeyError:
                    continue
                if member.isdir():
                    continue

                parts = [p for p in rel_path.split("/") if p]
                if not parts:
                    continue
                *folder_parts, filename = parts
                parent_id = ensure_subfolder(folder_parts)

                src_f = src_tar.extractfile(member)
                if not src_f:
                    continue

                data_bytes = src_f.read()
                mime_type = guess_mime_type(filename)

                media = MediaIoBaseUpload(
                    io.BytesIO(data_bytes),
                    mimetype=mime_type,
                    resumable=False,
                )
                file_meta = {
                    "name": filename,
                    "parents": [parent_id],
                }
                created = (
                    service.files()
                    .create(
                        body=file_meta,
                        media_body=media,
                        fields="id, name",
                    )
                    .execute()
                )
                uploaded.append({"path": rel_path, "drive_id": created["id"]})

    return jsonify(
        {
            "ok": True,
            "uploaded": uploaded,
            "restore_root_id": restore_root_id,
        }
    )

@admin_bp.route("/dashboard")
def dashboard():
    """
    Tela inicial do produto (Home / Dashboard).
    Mostra:
      - Últimos backups
      - Próximos agendamentos
      - Status atual das tarefas
      - Resumo de credencial Google
    """
    creds = get_credentials()
    if not creds:
        flash("Faça login no Google primeiro.")
        return redirect(url_for("auth.index"))

    # --- Métricas principais ---
    total_backups = BackupFileModel.query.count()
    total_tasks = TaskModel.query.count()
    total_schedules = ScheduledTaskModel.query.count()
    active_schedules = ScheduledTaskModel.query.filter_by(active=True).count()

    # Soma de tamanho total dos backups (MB)
    total_size_mb = (
        db.session.query(func.coalesce(func.sum(BackupFileModel.size_mb), 0.0))
        .scalar()
        or 0.0
    )

    # Último backup salvo
    last_backup = (
        BackupFileModel.query
        .order_by(BackupFileModel.created_at.desc())
        .first()
    )

    # Últimos 5 backups para a lista "Últimos backups"
    latest_backups = (
        BackupFileModel.query
        .order_by(BackupFileModel.created_at.desc())
        .limit(5)
        .all()
    )

    # Próximos agendamentos (somente ativos)
    next_schedule = (
        ScheduledTaskModel.query
        .filter(ScheduledTaskModel.active.is_(True))
        .filter(ScheduledTaskModel.next_run_at.isnot(None))
        .order_by(ScheduledTaskModel.next_run_at.asc())
        .first()
    )

    upcoming_schedules = (
        ScheduledTaskModel.query
        .filter(ScheduledTaskModel.active.is_(True))
        .order_by(ScheduledTaskModel.next_run_at.asc())
        .limit(5)
        .all()
    )

    # Tarefas mais recentes (para o card "Status atual das tarefas")
    latest_tasks = (
        TaskModel.query
        .order_by(TaskModel.updated_at.desc())
        .limit(6)
        .all()
    )

    # Sucesso x Erro para o mini-gráfico
    tasks_success = TaskModel.query.filter_by(phase="concluido").count()
    tasks_error = TaskModel.query.filter_by(phase="erro").count()
    total_finished = tasks_success + tasks_error

    success_percent = 0
    error_percent = 0
    if total_finished > 0:
        success_percent = int((tasks_success / total_finished) * 100)
        error_percent = 100 - success_percent

    # Status da credencial Google (email, último refresh)
    from app.models import GoogleAuthModel  # já é importado no topo em sua versão atual
    google_auth = (
        GoogleAuthModel.query
        .filter_by(active=True)
        .order_by(GoogleAuthModel.updated_at.desc())
        .first()
    )

    return render_template(
        "dashboard.html",
        total_backups=total_backups,
        total_size_mb=round(total_size_mb, 2),
        total_tasks=total_tasks,
        total_schedules=total_schedules,
        active_schedules=active_schedules,
        last_backup=last_backup,
        latest_backups=latest_backups,
        next_schedule=next_schedule,
        upcoming_schedules=upcoming_schedules,
        latest_tasks=latest_tasks,
        tasks_success=tasks_success,
        tasks_error=tasks_error,
        success_percent=success_percent,
        error_percent=error_percent,
        google_auth=google_auth,
    )