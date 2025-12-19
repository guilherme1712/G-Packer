# services/drive_download_service.py
import errno
import os
import io
import stat
import time
import shutil
import tempfile
import zipfile
import tarfile
import py7zr  # <--- NOVA IMPORTAÇÃO
import threading
import concurrent.futures
import random
import queue
import re

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from .drive_tree import build_files_list_for_items, get_children
from .drive import file_passes_filters
from app.services.progress import sync_task_to_db, update_progress
from app.services.storage import StorageService
from concurrent.futures import as_completed
from app.services.worker_manager import WorkerManager

_dl_lock = threading.Lock()
_archive_lock = threading.Lock()
_thread_local = threading.local()

CHUNK_SIZE = 50 * 1024 * 1024
RETRY_LIMIT = 10
MEMORY_BUFFER_LIMIT = 100 * 1024 * 1024


def safe_name(name):
    """
    Higieniza nomes para Windows.
    """
    if not name:
        return "sem_nome"
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = "".join(c for c in name if c.isprintable())
    name = name.strip().rstrip('.')
    if not name:
        return "unnamed"
    return name


def get_thread_safe_service(creds):
    if not hasattr(_thread_local, "service"):
        _thread_local.service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _thread_local.service


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


def format_size(size_bytes):
    if not size_bytes: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def get_export_info(mime_type: str, file_name: str):
    if not mime_type: return (None, None)
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


def _worker_download_one(creds, f_info, dest_root, used_rel_paths, progress_dict, task_id, filters):
    service = get_thread_safe_service(creds)
    file_id = f_info["id"]
    mime = f_info.get("mimeType") or ""
    original_name = f_info.get("name") or "arquivo"
    file_size_bytes = f_info.get("size_bytes", 0)

    raw_rel_path = f_info.get("rel_path")
    final_parts = []
    if raw_rel_path:
        parts = raw_rel_path.replace('\\', '/').split('/')
        final_parts = [safe_name(p) for p in parts]
    else:
        final_parts = [safe_name(original_name)]

    sanitized_rel_path = os.path.join(*final_parts)
    download_name = final_parts[-1]
    rel_dir = os.path.dirname(sanitized_rel_path)

    check_status_pause_cancel(progress_dict, task_id)

    if mime == "application/vnd.google-apps.shortcut":
        try:
            sc_meta = service.files().get(fileId=file_id, fields="shortcutDetails").execute()
            target = sc_meta.get("shortcutDetails", {}).get("targetId")
            if target:
                meta = service.files().get(fileId=target, fields="id,name,mimeType,size").execute()
                file_id = meta["id"]
                mime = meta["mimeType"] or ""
                download_name = safe_name(meta["name"])
                if rel_dir:
                    sanitized_rel_path = os.path.join(rel_dir, download_name)
                else:
                    sanitized_rel_path = download_name
                file_size_bytes = int(meta.get("size", 0))
                if filters and not file_passes_filters(meta, filters):
                    return
            else:
                return
        except Exception:
            return

    request_dl = None
    export_mime = None

    try:
        if mime.startswith("application/vnd.google-apps."):
            export_mime, new_name = get_export_info(mime, download_name)
            if export_mime:
                download_name = safe_name(new_name)
                request_dl = service.files().export_media(fileId=file_id, mimeType=export_mime)
            else:
                return
        else:
            request_dl = service.files().get_media(fileId=file_id)
    except Exception as e:
        with _dl_lock:
            if progress_dict and task_id:
                info = progress_dict[task_id]
                hist = info.get("history", [])
                hist.append(f"FALHA Meta {download_name}: {str(e)}")
                info["history"] = hist
        return

    with _dl_lock:
        dir_part = os.path.dirname(sanitized_rel_path)
        base_part = os.path.basename(sanitized_rel_path)
        candidate_rel = sanitized_rel_path
        root, ext = os.path.splitext(base_part)
        counter = 1
        while candidate_rel in used_rel_paths:
            new_base = f"{root} ({counter}){ext}"
            candidate_rel = os.path.join(dir_part, new_base)
            counter += 1
        used_rel_paths.add(candidate_rel)
        final_rel_path = candidate_rel

    raw_local_path = os.path.join(dest_root, final_rel_path)
    local_path = StorageService.prepare_long_path(raw_local_path)
    dir_name = os.path.dirname(local_path)
    StorageService.ensure_dir(dir_name)

    fh = io.FileIO(local_path, "wb")
    try:
        downloader = MediaIoBaseDownload(fh, request_dl, chunksize=CHUNK_SIZE)
        done = False
        retry_count = 0
        while not done:
            check_status_pause_cancel(progress_dict, task_id)
            try:
                status, done = downloader.next_chunk()
                retry_count = 0
            except HttpError as err:
                if err.resp.status in [403, 429, 500, 502, 503]:
                    retry_count += 1
                    if retry_count > RETRY_LIMIT: raise err
                    sleep_s = (2 ** retry_count) + random.uniform(0, 1)
                    time.sleep(sleep_s)
                    continue
                else:
                    raise err
            except Exception as e:
                if "Cancelado" in str(e): raise e
                retry_count += 1
                if retry_count > RETRY_LIMIT: raise e
                time.sleep(2)
                continue
    except Exception as e:
        if not fh.closed: fh.close()
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except:
                pass
        if "Cancelado" in str(e): raise e
        with _dl_lock:
            if progress_dict and task_id:
                info = progress_dict[task_id]
                info["errors"] = info.get("errors", 0) + 1
                hist = info.get("history", [])
                hist.append(f"FALHA DL {download_name}: {str(e)}")
                info["history"] = hist
                progress_dict[task_id] = info
        return
    finally:
        if not fh.closed: fh.close()

    f_info["local_rel_path"] = final_rel_path

    with _dl_lock:
        if progress_dict and task_id:
            info = progress_dict[task_id]
            dl_now = info.get("files_downloaded", 0) + 1
            info["files_downloaded"] = dl_now
            info["bytes_downloaded"] = info.get("bytes_downloaded", 0) + file_size_bytes
            total_seen = info.get("files_total", 0)
            info["message"] = f"Baixando ({dl_now}/{total_seen})"
            if dl_now % 20 == 0:
                hist = info.get("history", [])
                size_str = format_size(file_size_bytes)
                hist.append(f"Baixado: {download_name} ({size_str})")
                info["history"] = hist
            progress_dict[task_id] = info


def _concurrent_mapper(creds, items, q, progress_dict, task_id, filters):
    try:
        service = get_thread_safe_service(creds)
        stack = []
        for item in items:
            safe = safe_name(item["name"])
            stack.append({
                "id": item["id"],
                "name": item["name"],
                "type": item["type"],
                "rel_path": safe
            })

        while stack:
            check_status_pause_cancel(progress_dict, task_id)
            current = stack.pop(0)

            if current["type"] == "file":
                try:
                    meta = service.files().get(fileId=current["id"], fields="id,name,mimeType,size").execute()
                    current["size_bytes"] = int(meta.get("size", 0))
                    current["mimeType"] = meta.get("mimeType")
                except:
                    current["size_bytes"] = 0
                q.put(current)
                with _dl_lock:
                    if progress_dict and task_id:
                        info = progress_dict[task_id]
                        info["files_total"] = info.get("files_total", 0) + 1
                        info["bytes_found"] = info.get("bytes_found", 0) + current.get("size_bytes", 0)
                        info["message"] = f"Mapeando... ({info['files_total']} enc.)"
                        progress_dict[task_id] = info

            elif current["type"] == "folder":
                children = get_children(creds, current["id"], include_files=True)
                for child in children:
                    check_status_pause_cancel(progress_dict, task_id)
                    child_clean_name = safe_name(child["name"])
                    child_rel = os.path.join(current["rel_path"], child_clean_name)
                    if child["type"] == "folder":
                        stack.append({
                            "id": child["id"],
                            "name": child["name"],
                            "type": "folder",
                            "rel_path": child_rel
                        })
                    else:
                        if filters and not file_passes_filters(child, filters):
                            continue
                        file_obj = {
                            "id": child["id"],
                            "name": child["name"],
                            "mimeType": child.get("mimeType"),
                            "rel_path": child_rel,
                            "type": "file",
                            "size_bytes": child.get("size_bytes", 0)
                        }
                        q.put(file_obj)
                        with _dl_lock:
                            if progress_dict and task_id:
                                info = progress_dict[task_id]
                                info["files_total"] = info.get("files_total", 0) + 1
                                info["bytes_found"] = info.get("bytes_found", 0) + file_obj["size_bytes"]
                                info["message"] = f"Mapeando... ({info['files_total']} enc.)"
                                progress_dict[task_id] = info
    except Exception as e:
        with _dl_lock:
            if progress_dict and task_id:
                hist = progress_dict[task_id].get("history", [])
                hist.append(f"Erro no mapeamento: {str(e)}")
                progress_dict[task_id]["history"] = hist


def _concurrent_worker(creds, q, dest_root, used_rel_paths, progress_dict, task_id, filters, results_list):
    get_thread_safe_service(creds)
    while True:
        try:
            item = q.get(timeout=2)
        except queue.Empty:
            return
        if item is None:
            break
        try:
            _worker_download_one(creds, item, dest_root, used_rel_paths, progress_dict, task_id, filters)
            with _dl_lock:
                results_list.append(item)
        except Exception as e:
            pass
        finally:
            q.task_done()


def execute_concurrent_download(creds, items, dest_root, progress_dict, task_id, filters):
    file_queue = queue.Queue()
    results_list = []
    used_rel_paths = set()
    mapper_thread = threading.Thread(
        target=_concurrent_mapper,
        args=(creds, items, file_queue, progress_dict, task_id, filters)
    )
    mapper_thread.start()
    num_workers = WorkerManager.MAX_DOWNLOAD_WORKERS
    executor = WorkerManager.get_download_executor()
    worker_futures = []
    for _ in range(num_workers):
        fut = executor.submit(
            _concurrent_worker, creds, file_queue, dest_root, used_rel_paths, progress_dict, task_id, filters, results_list
        )
        worker_futures.append(fut)
    mapper_thread.join()
    file_queue.join()
    for _ in range(num_workers):
        file_queue.put(None)
    for fut in worker_futures:
        try:
            fut.result()
        except Exception:
            pass
    return results_list


def _worker_archive_one(f_info, tmp_root, archive_obj, archive_format, progress_dict, task_id):
    """
    Adiciona um arquivo ao archive (ZIP, TAR ou 7Z).
    """
    rel = f_info.get("local_rel_path")
    if not rel: return

    src = os.path.join(tmp_root, rel)
    src_long = StorageService.prepare_long_path(src)

    if not os.path.exists(src_long):
        return

    check_status_pause_cancel(progress_dict, task_id)

    # Corrige barras para padrões UNIX
    arcname_fixed = rel.replace(os.sep, "/")

    try:
        file_size = os.path.getsize(src_long)
        file_content = None

        # Otimização de memória para arquivos pequenos
        if file_size < MEMORY_BUFFER_LIMIT:
            with open(src_long, "rb") as f:
                file_content = f.read()

        # --- BLOQUEIO PARA ESCRITA NO ARQUIVO ---
        with _archive_lock:
            if archive_format == "zip":
                if file_content is not None:
                    archive_obj.writestr(arcname_fixed, file_content)
                else:
                    archive_obj.write(src_long, arcname=arcname_fixed)

            elif archive_format == "7z":
                # py7zr suporta escrita
                if file_content is not None:
                    archive_obj.writestr(file_content, arcname_fixed)
                else:
                    archive_obj.write(src_long, arcname=arcname_fixed)

            else:  # tar.gz
                archive_obj.add(src_long, arcname=arcname_fixed)

    except Exception as e:
        print(f"Erro ao compactar {rel}: {e}")


def download_files_to_folder(creds, files_list, dest_root, progress_dict=None, task_id=None, filters=None, processing_mode="sequential"):
    if not files_list: return
    total = len(files_list)
    update_progress(task_id, {
        "phase": "baixando",
        "files_downloaded": 0,
        "files_total": total,
        "bytes_downloaded": 0,
        "message": f"Iniciando download de {total} itens (Sequencial)...",
        "history": ["Iniciando downloads..."]
    })
    sync_task_to_db(task_id)
    used_rel_paths = set()
    changes_since_sync = 0
    executor = WorkerManager.get_download_executor()
    futures = []
    try:
        for f_info in files_list:
            fut = executor.submit(
                _worker_download_one, creds, f_info, dest_root, used_rel_paths, progress_dict, task_id, filters
            )
            futures.append(fut)
        for future in as_completed(futures):
            try:
                future.result()
                changes_since_sync += 1
                if progress_dict and task_id and changes_since_sync >= 25:
                    sync_task_to_db(task_id)
                    changes_since_sync = 0
            except Exception as exc:
                if "Cancelado" in str(exc):
                    for f in futures: f.cancel()
                    sync_task_to_db(task_id)
                    raise exc
    except Exception as e:
        raise e
    if progress_dict and task_id:
        sync_task_to_db(task_id)


def handle_remove_readonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        func(path)
    else:
        pass


def download_items_bundle(
        creds: Credentials,
        items: list,
        base_name: str,
        compression_level: str = "normal",
        archive_format: str = "zip",
        progress_dict=None,
        task_id: str | None = None,
        filters: dict | None = None,
        processing_mode: str = "sequential",
        previous_manifest: dict | None = None
):
    """
    Suporta 'zip', 'tar' e '7z'.
    """
    local_temp_base = StorageService.temp_work_dir()
    tmp_root = tempfile.mkdtemp(prefix="dl_", dir=local_temp_base)

    files_to_download = []
    full_manifest = {}

    try:
        check_status_pause_cancel(progress_dict, task_id)
        if previous_manifest is not None:
            processing_mode = "sequential"

        if processing_mode == "concurrent":
            update_progress(task_id, {
                "phase": "mapeando",
                "message": "MODO TURBO: Mapeando e Baixando simultaneamente...",
                "files_total": 0, "bytes_found": 0, "bytes_downloaded": 0,
                "history": ["Iniciando Modo Concorrente..."]
            })
            execute_concurrent_download(creds, items, tmp_root, progress_dict, task_id, filters)
        else:
            service_main = build("drive", "v3", credentials=creds)
            all_files = build_files_list_for_items(service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id)

            for f in all_files:
                fid = f['id']
                mtime = f.get('modifiedTime')
                full_manifest[fid] = mtime
                should_download = True
                if previous_manifest is not None:
                    prev_time = previous_manifest.get(fid)
                    if prev_time and mtime <= prev_time:
                        should_download = False
                if should_download:
                    files_to_download.append(f)

            if not files_to_download and previous_manifest is None:
                raise Exception("Nenhum arquivo encontrado.")

            if not files_to_download and previous_manifest is not None:
                update_progress(task_id, {"message": "Nenhuma alteração encontrada (Incremental vazio)."})
            else:
                download_files_to_folder(
                    creds, files_to_download, dest_root=tmp_root,
                    progress_dict=progress_dict, task_id=task_id,
                    filters=filters, processing_mode=processing_mode
                )

        check_status_pause_cancel(progress_dict, task_id)

        update_progress(task_id, {
            "phase": "compactando", "message": "Compactando...",
            "history": [f"Compactando {len(files_to_download)} arquivos para {archive_format}..."]
        })
        sync_task_to_db(task_id)

        out_dir = tempfile.mkdtemp(prefix="out_", dir=local_temp_base)
        base_name = safe_name(base_name if base_name else "backup")

        # --- DEFINIÇÃO DA EXTENSÃO E OBJETO ---
        if archive_format == "zip":
            ext = "zip"
        elif archive_format == "7z":
            ext = "7z"
        else:
            ext = "tar.gz"

        archive_path = os.path.join(out_dir, f"{base_name}.{ext}")

        archive_obj = None

        try:
            # --- INICIALIZAÇÃO DO COMPACTADOR ---
            if archive_format == "zip":
                lvl = 1 if compression_level == "fast" else (9 if compression_level == "max" else 6)
                archive_obj = zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=lvl, allowZip64=True)


            elif archive_format == "7z":
                # CORREÇÃO: Usar inteiros em vez de constantes que podem não existir
                # 1 = Fast, 5 = Normal, 9 = Ultra
                preset = 5
                if compression_level == "fast":
                    preset = 1

                elif compression_level == "max":
                    preset = 9

                # Cria o arquivo 7z com o filtro LZMA2 e o preset definido
                archive_obj = py7zr.SevenZipFile(
                    archive_path,
                    'w',
                    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': preset}]
                )

            else:
                archive_obj = tarfile.open(archive_path, "w:gz")

            executor = WorkerManager.get_compression_executor()
            futures = []

            for f_info in files_to_download:
                futures.append(executor.submit(
                    _worker_archive_one, f_info, tmp_root, archive_obj, archive_format, progress_dict, task_id
                ))

            for future in as_completed(futures):
                future.result()

        finally:
            if archive_obj: archive_obj.close()

    except Exception as e:
        shutil.rmtree(StorageService.prepare_long_path(tmp_root), onerror=handle_remove_readonly)
        if progress_dict and task_id: sync_task_to_db(task_id)
        raise e

    shutil.rmtree(StorageService.prepare_long_path(tmp_root), onerror=handle_remove_readonly)

    update_progress(task_id, {"phase": "concluido", "message": "Sucesso!", "history": ["Finalizado."]})
    sync_task_to_db(task_id)

    return archive_path, full_manifest


def mirror_items_to_local(creds, items, dest_root, progress_dict=None, task_id=None, filters=None, processing_mode="sequential"):
    dest_root_long = StorageService.prepare_long_path(dest_root)
    StorageService.ensure_dir(dest_root_long)
    check_status_pause_cancel(progress_dict, task_id)

    if processing_mode == "concurrent":
        update_progress(task_id, {
            "phase": "mapeando", "message": "Espelho Turbo...",
            "files_total": 0, "bytes_found": 0, "bytes_downloaded": 0,
        })
        execute_concurrent_download(creds, items, dest_root, progress_dict, task_id, filters)
    else:
        service_main = build("drive", "v3", credentials=creds)
        files_list = build_files_list_for_items(service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id)
        if not files_list:
            raise Exception("Nenhum arquivo encontrado.")
        download_files_to_folder(creds, files_list, dest_root=dest_root, progress_dict=progress_dict, task_id=task_id, filters=filters,
                                 processing_mode=processing_mode)

    update_progress(task_id, {"phase": "concluido", "message": f"Espelho ok: {dest_root}", "history": ["Espelho concluído."]})
    sync_task_to_db(task_id)