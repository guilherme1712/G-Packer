import os
import time
import json
import shutil  # <--- IMPORTANTE: Necessário para mover o arquivo final
from threading import Thread

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
    send_from_directory,
    flash,
    current_app
)

from services.auth_service import get_credentials
from services.drive_filters import build_filters_from_form
from services.drive_tree_service import get_children, get_file_metadata
from services.drive_download_service import (
    download_items_bundle,
    mirror_items_to_local,
)
from services.progress_service import (
    PROGRESS,
    init_download_task,
    get_task_progress,
    sync_task_to_db
)

drive_bp = Blueprint("drive", __name__)

# Nome da pasta física onde salvaremos os arquivos ZIP
BACKUP_FOLDER_NAME = 'storage/backups'

# -----------------------------------------------------------
# Função auxiliar de Thread (Executada em Segundo Plano)
# -----------------------------------------------------------
def _background_backup_task(app_context, task_id, creds, items, output_mode,
                          local_mirror_path, storage_root_path, zip_file_name,
                          compression_level, archive_format, filters):
    """
    Função isolada que roda na thread.
    """
    # Ativa o contexto da aplicação para ter acesso ao DB
    with app_context:
        try:
            if output_mode == "mirror":
                if not local_mirror_path:
                    raise Exception("Caminho local inválido")

                mirror_items_to_local(
                    creds, items,
                    dest_root=local_mirror_path,
                    progress_dict=PROGRESS, task_id=task_id, filters=filters
                )
                PROGRESS[task_id]["message"] = "Espelhamento concluído."

            else:
                # Modo Arquivo (ZIP)
                # 1. Gera o ZIP em uma pasta temporária (retorna o caminho completo do temp)
                # Passamos apenas o NOME do arquivo, não o caminho completo.
                temp_zip_path = download_items_bundle(
                    creds, items,
                    base_name=zip_file_name,
                    compression_level=compression_level,
                    archive_format=archive_format,
                    progress_dict=PROGRESS, task_id=task_id, filters=filters
                )

                # 2. Define o destino final
                # Garante que pegamos apenas o nome do arquivo gerado (ex: backup.zip)
                generated_filename = os.path.basename(temp_zip_path)
                final_dest_path = os.path.join(storage_root_path, generated_filename)

                # 3. Move do Temp para a pasta storage_backups
                shutil.move(temp_zip_path, final_dest_path)

                # 4. Atualiza o progresso com o nome final correto
                PROGRESS[task_id]["final_filename"] = generated_filename
                PROGRESS[task_id]["message"] = "Arquivo gerado e salvo com sucesso."

            # Opcional: sync_task_to_db(task_id)

        except Exception as e:
            print(f"Erro na thread de backup {task_id}: {e}")
            PROGRESS[task_id]["phase"] = "erro"
            PROGRESS[task_id]["message"] = str(e)
            if "history" not in PROGRESS[task_id]:
                PROGRESS[task_id]["history"] = []
            PROGRESS[task_id]["history"].append(f"ERRO: {str(e)}")


# -----------------------------------------------------------
# UI principal
# -----------------------------------------------------------
@drive_bp.route("/folders")
def folders():
    creds = get_credentials()
    if not creds:
        flash("Faça login no Google primeiro.")
        return redirect(url_for("auth.index"))
    return render_template("folders.html")


# -----------------------------------------------------------
# APIs para árvore de pastas/arquivos
# -----------------------------------------------------------
@drive_bp.route("/api/folders/root")
def api_folders_root():
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    include_files = request.args.get("files") == "1"
    items = get_children(creds, "root", include_files=include_files)
    return jsonify({"items": items})


@drive_bp.route("/api/folders/children/<folder_id>")
def api_folders_children(folder_id):
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    include_files = request.args.get("files") == "1"
    items = get_children(creds, folder_id, include_files=include_files)
    return jsonify({"items": items})


@drive_bp.route("/api/file/<file_id>")
def api_file_details(file_id):
    """Retorna metadados de um arquivo específico para o painel de preview."""
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    meta = get_file_metadata(creds, file_id)
    return jsonify(meta)


# -----------------------------------------------------------
# Download / Processamento (POST JSON)
# -----------------------------------------------------------
@drive_bp.route("/download", methods=["POST"])
def download():
    # 1. Coleta e valida dados no contexto da Request principal
    creds = get_credentials()
    if not creds:
        return jsonify({"ok": False, "error": "Sessão expirada"}), 401

    data = request.get_json() or {}

    items_raw = data.get("items_json")
    items = []
    if isinstance(items_raw, str):
        try:
            items = json.loads(items_raw)
        except:
            items = []
    else:
        items = items_raw

    if not items:
        return jsonify({"ok": False, "error": "Nenhum item selecionado"}), 400

    # 2. Extrai parâmetros simples (Strings/Ints)
    zip_name = (data.get("zip_name") or "backup").strip()
    # Remove extensão se o usuário digitou, para evitar backup.zip.zip
    if zip_name.lower().endswith('.zip'):
        zip_name = zip_name[:-4]

    archive_format = data.get("archive_format") or "zip"
    compression_level = data.get("compression_level") or "normal"
    output_mode = data.get("output_mode") or "archive"
    local_mirror_path = (data.get("local_mirror_path") or "").strip()
    execution_mode = data.get("execution_mode") or "immediate"

    task_id = data.get("task_id")
    if not task_id:
        task_id = f"task-{int(time.time())}"

    # 3. Processa filtros
    filters = build_filters_from_form(data)

    # 4. Configura caminhos (Ainda no contexto da app)
    # Apenas criamos a pasta, mas NÃO passamos o caminho completo como nome do arquivo
    storage_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)
    os.makedirs(storage_path, exist_ok=True)

    # 5. Inicializa Task em memória
    init_download_task(task_id)
    if task_id in PROGRESS:
        PROGRESS[task_id]["history"] = []
        PROGRESS[task_id]["canceled"] = False
        PROGRESS[task_id]["output_mode"] = output_mode
        PROGRESS[task_id]["final_filename"] = None

    # 6. Captura o Contexto da Aplicação (App Context)
    app_ctx = current_app.app_context()

    # 7. Inicia a Thread
    # CORREÇÃO: Passamos storage_path e zip_name separadamente
    t = Thread(target=_background_backup_task, args=(
        app_ctx,
        task_id,
        creds,
        items,
        output_mode,
        local_mirror_path,
        storage_path, # Caminho da pasta de destino
        zip_name,     # Apenas o nome do arquivo
        compression_level,
        archive_format,
        filters
    ))
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

    # Se concluído e for arquivo, gera URL de download
    if data.get("phase") == "concluido":
        filename = PROGRESS.get(task_id, {}).get("final_filename")
        if filename:
            # url_for funciona aqui pois estamos numa request HTTP normal de polling
            data["download_url"] = url_for("drive.get_file", filename=filename)
            data["filename"] = filename

    return jsonify(data)


@drive_bp.route("/drive/get-file/<path:filename>")
def get_file(filename):
    """Rota para baixar o arquivo gerado que está na pasta storage_backups."""
    # Opcional: Verificar credenciais novamente para segurança
    creds = get_credentials()
    if not creds:
        flash("Sessão expirada.")
        return redirect(url_for("auth.index"))

    storage_path = os.path.join(current_app.root_path, BACKUP_FOLDER_NAME)

    # Adicionei print para debug no console se der erro novamente
    full_p = os.path.join(storage_path, filename)
    if not os.path.exists(full_p):
        print(f"ERRO 404: Arquivo não encontrado no disco: {full_p}")

    return send_from_directory(storage_path, filename, as_attachment=True)


@drive_bp.route("/cancel/<task_id>", methods=["POST"])
def cancel_task(task_id):
    if task_id in PROGRESS:
        PROGRESS[task_id]["canceled"] = True
        history = PROGRESS[task_id].get("history", [])
        history.append("Solicitando cancelamento...")
        PROGRESS[task_id]["history"] = history[-50:]
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Task not found"}), 404
