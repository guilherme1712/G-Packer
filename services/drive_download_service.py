# services/drive_download_service.py
import os
import io
import time
import shutil
import tempfile
import zipfile
import tarfile
import threading
import concurrent.futures
import random

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from .drive_filters import safe_name, file_passes_filters
from .drive_tree_service import build_files_list_for_items
from .progress_service import sync_task_to_db, update_progress

_dl_lock = threading.Lock()

# Configuração de alta performance
MAX_DOWNLOAD_WORKERS = 20         # Aumentado para 20 downloads paralelos
CHUNK_SIZE = 20 * 1024 * 1024     # 20 MB por chunk (Reduz requests HTTP, muito mais rápido)
RETRY_LIMIT = 10                  # Mais tentativas para downloads

def prepare_long_path(path: str) -> str:
    if os.name == "nt":
        path = os.path.abspath(path)
        if not path.startswith("\\\\?\\"):
            return f"\\\\?\\{path}"
    return path


def check_status_pause_cancel(progress_dict, task_id):
    if not progress_dict or not task_id:
        return

    while True:
        info = progress_dict.get(task_id, {})
        if info.get("canceled"):
            raise Exception("Cancelado pelo usuário")

        if info.get("paused"):
            time.sleep(1)
            continue
        break


def get_export_info(mime_type: str, file_name: str):
    if mime_type == "application/vnd.google-apps.document":
        return ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                file_name if file_name.lower().endswith(".docx") else file_name + ".docx")
    if mime_type == "application/vnd.google-apps.spreadsheet":
        return ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                file_name if file_name.lower().endswith(".xlsx") else file_name + ".xlsx")
    if mime_type == "application/vnd.google-apps.presentation":
        return ("application/vnd.openxmlformats-officedocument.presentationml.presentation",
                file_name if file_name.lower().endswith(".pptx") else file_name + ".pptx")
    if mime_type == "application/vnd.google-apps.drawing":
        return ("image/png",
                file_name if file_name.lower().endswith(".png") else file_name + ".png")
    return (None, None)


def _worker_download_one(
    creds,
    f_info,
    dest_root,
    used_rel_paths,
    progress_dict,
    task_id,
    filters,
):
    # Cria serviço dedicado à thread (melhor para paralelismo)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    file_id = f_info["id"]
    mime = f_info.get("mimeType", "")
    original_name = f_info.get("name") or "arquivo"
    raw_rel_path = f_info.get("rel_path") or safe_name(original_name)
    rel_dir = os.path.dirname(raw_rel_path)
    download_name = original_name

    check_status_pause_cancel(progress_dict, task_id)

    # Tratamento de Atalhos
    if mime == "application/vnd.google-apps.shortcut":
        try:
            sc_meta = service.files().get(fileId=file_id, fields="shortcutDetails").execute()
            target = sc_meta.get("shortcutDetails", {}).get("targetId")
            if target:
                meta = service.files().get(fileId=target, fields="id,name,mimeType,size").execute()
                file_id = meta["id"]
                mime = meta["mimeType"]
                download_name = meta["name"]
                if filters and not file_passes_filters(meta, filters):
                    return
            else:
                return
        except Exception:
            return

    # Tenta obter o Request de Download com Retry
    request_dl = None
    export_mime = None

    try:
        if mime.startswith("application/vnd.google-apps."):
            export_mime, new_name = get_export_info(mime, download_name)
            if export_mime:
                download_name = new_name
                request_dl = service.files().export_media(fileId=file_id, mimeType=export_mime)
            else:
                return
        else:
            request_dl = service.files().get_media(fileId=file_id)
    except Exception as e:
        # Se falhar na criação do request, loga e sai
        with _dl_lock:
             if progress_dict and task_id:
                info = progress_dict[task_id]
                hist = info.get("history", [])
                hist.append(f"FALHA Meta {download_name}: {str(e)}")
                info["history"] = hist
        return

    # Define caminhos locais
    with _dl_lock:
        base_name_safe = safe_name(download_name)
        candidate_rel = os.path.join(rel_dir, base_name_safe) if rel_dir else base_name_safe

        root, ext = os.path.splitext(candidate_rel)
        counter = 1
        while candidate_rel in used_rel_paths:
            candidate_rel = f"{root} ({counter}){ext}"
            counter += 1

        used_rel_paths.add(candidate_rel)
        final_rel_path = candidate_rel
        raw_local_path = os.path.join(dest_root, final_rel_path)
        local_path = prepare_long_path(raw_local_path)
        dir_name = os.path.dirname(local_path)
        os.makedirs(dir_name, exist_ok=True)

    # Executa Download com Chunking e Retry Robusto
    fh = io.FileIO(local_path, "wb")
    try:
        downloader = MediaIoBaseDownload(fh, request_dl, chunksize=CHUNK_SIZE)
        done = False

        retry_count = 0
        while not done:
            check_status_pause_cancel(progress_dict, task_id)

            try:
                status, done = downloader.next_chunk()
                # Reset retry count após sucesso de um chunk
                retry_count = 0

            except HttpError as err:
                # Trata Limite de Taxa (Rate Limit)
                if err.resp.status in [403, 429, 500, 502, 503]:
                    retry_count += 1
                    if retry_count > RETRY_LIMIT:
                        raise err

                    sleep_s = (2 ** retry_count) + random.uniform(0, 1)
                    print(f"Download Rate Limit ({download_name}). Esperando {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
                    # O downloader do googleapiclient geralmente é inteligente o suficiente
                    # para retomar, mas precisamos manter o loop.
                    continue
                else:
                    raise err

            except Exception as e:
                # Outros erros de rede
                if "Cancelado" in str(e): raise e

                retry_count += 1
                if retry_count > RETRY_LIMIT:
                    raise e
                time.sleep(2)
                continue

    except Exception as e:
        if not fh.closed: fh.close()
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except: pass

        if "Cancelado" in str(e): raise e

        with _dl_lock:
            if progress_dict and task_id:
                info = progress_dict[task_id]
                info["errors"] = info.get("errors", 0) + 1
                hist = info.get("history", [])
                hist.append(f"FALHA Download {download_name}: {str(e)}")
                info["history"] = hist
        return
    finally:
        if not fh.closed: fh.close()

    f_info["local_rel_path"] = final_rel_path

    # Atualiza progresso
    with _dl_lock:
        if progress_dict and task_id:
            info = progress_dict[task_id]
            dl_now = info.get("files_downloaded", 0) + 1
            info["files_downloaded"] = dl_now
            total = info.get("files_total", 0)

            # Só notifica a cada X arquivos para não travar a UI/DB com updates excessivos
            if dl_now % 5 == 0 or dl_now == total:
                info["message"] = f"Baixando ({dl_now}/{total})..."
                # Opcional: não encher o histórico com cada arquivo se forem muitos
                # hist = info.get("history", [])
                # hist.append(f"Baixado: {download_name}")
                # info["history"] = hist
                progress_dict[task_id] = info


def download_files_to_folder(
    creds,
    files_list: list[dict],
    dest_root: str,
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
) -> None:
    if not files_list:
        return

    total = len(files_list)
    update_progress(task_id, {
        "phase": "baixando",
        "files_downloaded": 0,
        "files_total": total,
        "message": f"Iniciando download turbo ({MAX_DOWNLOAD_WORKERS} threads) de {total} itens...",
        "history": [f"Iniciando downloads (Max Threads: {MAX_DOWNLOAD_WORKERS})..."]
    })
    sync_task_to_db(task_id)

    used_rel_paths = set()
    changes_since_sync = 0

    # ThreadPool Executor com mais workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
        futures = []
        for f_info in files_list:
            fut = executor.submit(
                _worker_download_one, creds, f_info, dest_root, used_rel_paths, progress_dict, task_id, filters
            )
            futures.append(fut)

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                changes_since_sync += 1
                # Reduzi a frequência de sync para dar mais performance
                if progress_dict and task_id and changes_since_sync >= 50:
                    sync_task_to_db(task_id)
                    changes_since_sync = 0
            except Exception as exc:
                if "Cancelado" in str(exc):
                    for f in futures: f.cancel()
                    sync_task_to_db(task_id)
                    raise exc
                else:
                    print(f"Erro na thread de download: {exc}")

    if progress_dict and task_id:
        sync_task_to_db(task_id)


def download_items_bundle(
    creds: Credentials,
    items: list,
    base_name: str,
    compression_level: str = "normal",
    archive_format: str = "zip",
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
    processing_mode: str = "parallel",
) -> str:
    service_main = build("drive", "v3", credentials=creds)

    # 1. Mapeamento
    files_list = build_files_list_for_items(
        service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id
    )

    if not files_list:
        raise Exception("Nenhum arquivo encontrado para download.")

    check_status_pause_cancel(progress_dict, task_id)

    local_temp_base = os.path.join(os.getcwd(), "storage/temp_work")
    os.makedirs(local_temp_base, exist_ok=True)
    tmp_root = tempfile.mkdtemp(prefix="dl_", dir=local_temp_base)

    try:
        # 2. Downloads
        download_files_to_folder(
            creds, files_list, dest_root=tmp_root, progress_dict=progress_dict, task_id=task_id, filters=filters
        )

        check_status_pause_cancel(progress_dict, task_id)

        # 3. Compactação
        update_progress(task_id, {
            "phase": "compactando",
            "message": "Compactando arquivos (Isso pode levar um tempo)...",
            "history": ["Iniciando compactação..."]
        })
        sync_task_to_db(task_id)

        out_dir = tempfile.mkdtemp(prefix="out_", dir=local_temp_base)
        if not base_name: base_name = "backup_drive"
        base_name = safe_name(base_name)

        if archive_format == "zip":
            archive_path = os.path.join(out_dir, f"{base_name}.zip")
            comp = zipfile.ZIP_DEFLATED
            level = 1 if compression_level == "fast" else (9 if compression_level == "max" else 6)

            # Usando ZIP_DEFLATED com allowZip64 para arquivos grandes
            with zipfile.ZipFile(archive_path, "w", compression=comp, compresslevel=level, allowZip64=True) as zf:
                for f_info in files_list:
                    check_status_pause_cancel(progress_dict, task_id)
                    rel = f_info.get("local_rel_path")
                    if not rel: continue
                    src = os.path.join(tmp_root, rel)
                    src_long = prepare_long_path(src)
                    if os.path.exists(src_long):
                        zf.write(src_long, arcname=rel)
        else:
            archive_path = os.path.join(out_dir, f"{base_name}.tar.gz")
            with tarfile.open(archive_path, "w:gz") as tf:
                for f_info in files_list:
                    check_status_pause_cancel(progress_dict, task_id)
                    rel = f_info.get("local_rel_path")
                    if not rel: continue
                    src = os.path.join(tmp_root, rel)
                    src_long = prepare_long_path(src)
                    if os.path.exists(src_long):
                        tf.add(src_long, arcname=rel)

    except Exception as e:
        shutil.rmtree(prepare_long_path(tmp_root), ignore_errors=True)
        if progress_dict and task_id:
            sync_task_to_db(task_id)
        raise e

    shutil.rmtree(prepare_long_path(tmp_root), ignore_errors=True)

    update_progress(task_id, {
        "phase": "concluido",
        "message": "Pacote gerado com sucesso!",
        "history": ["Processo finalizado com sucesso."]
    })
    sync_task_to_db(task_id)

    return archive_path


def mirror_items_to_local(
    creds: Credentials,
    items: list,
    dest_root: str,
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
    processing_mode: str = "parallel",
) -> None:
    service_main = build("drive", "v3", credentials=creds)

    files_list = build_files_list_for_items(
        service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id
    )
    if not files_list:
        raise Exception("Nenhum arquivo encontrado.")

    check_status_pause_cancel(progress_dict, task_id)

    dest_root_long = prepare_long_path(dest_root)
    os.makedirs(dest_root_long, exist_ok=True)

    download_files_to_folder(
        creds, files_list, dest_root=dest_root, progress_dict=progress_dict, task_id=task_id, filters=filters
    )

    update_progress(task_id, {
        "phase": "concluido",
        "message": f"Espelho atualizado em: {dest_root}",
        "history": [f"Espelho concluído em: {dest_root}"]
    })
    sync_task_to_db(task_id)
