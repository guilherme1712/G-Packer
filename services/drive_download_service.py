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
import queue

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from .drive_tree_service import build_files_list_for_items, get_children
from .drive_filters import safe_name, file_passes_filters
from .progress_service import sync_task_to_db, update_progress

_dl_lock = threading.Lock()

# Configuração de alta performance
MAX_DOWNLOAD_WORKERS = 40         
CHUNK_SIZE = 20 * 1024 * 1024     
RETRY_LIMIT = 10                  

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
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    file_id = f_info["id"]
    
    # --- CORREÇÃO DO BUG NoneType ---
    # Garante que mime seja sempre uma string, mesmo que venha None
    mime = f_info.get("mimeType") or "" 
    
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
                mime = meta["mimeType"] or "" # Proteção extra aqui também
                download_name = meta["name"]
                if filters and not file_passes_filters(meta, filters):
                    return
            else:
                return
        except Exception:
            return

    request_dl = None
    export_mime = None

    try:
        # Agora 'mime' é garantido ser string, então startswith não quebra
        if mime.startswith("application/vnd.google-apps."):
            export_mime, new_name = get_export_info(mime, download_name)
            if export_mime:
                download_name = new_name
                request_dl = service.files().export_media(fileId=file_id, mimeType=export_mime)
            else:
                # Se for um Google App não suportado para exportação (ex: Mapas, Scripts), ignora ou tenta binário
                return
        else:
            # Arquivos normais (MP4, PDF, JPG, etc) caem aqui
            request_dl = service.files().get_media(fileId=file_id)
            
    except Exception as e:
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

    # Executa Download
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
            try: os.remove(local_path)
            except: pass
        if "Cancelado" in str(e): raise e
        
        with _dl_lock:
            if progress_dict and task_id:
                info = progress_dict[task_id]
                info["errors"] = info.get("errors", 0) + 1
                hist = info.get("history", [])
                hist.append(f"FALHA {download_name}: {str(e)}")
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
            # No modo concorrente, files_total cresce dinamicamente
            total_seen = info.get("files_total", 0)
            
            info["message"] = f"Baixando: {dl_now}/{total_seen} itens..."
            
            # Não floodar o histórico
            if dl_now % 5 == 0:
                hist = info.get("history", [])
                hist.append(f"Baixado: {download_name}")
                info["history"] = hist
            
            progress_dict[task_id] = info
            
# =================================================================================
# LÓGICA DO MODO CONCORRENTE (PRODUTOR-CONSUMIDOR)
# =================================================================================

def _concurrent_mapper(creds, items, q, progress_dict, task_id, filters):
    """
    PRODUTOR: Navega nas pastas e coloca arquivos na fila (q) IMEDIATAMENTE.
    """
    try:
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
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
                q.put(current)
                with _dl_lock:
                    if progress_dict and task_id:
                        info = progress_dict[task_id]
                        info["files_total"] = info.get("files_total", 0) + 1
                        info["message"] = f"Mapeando... ({info['files_total']} encontrados)"
                        progress_dict[task_id] = info

            elif current["type"] == "folder":
                children = get_children(creds, current["id"], include_files=True)
                for child in children:
                    check_status_pause_cancel(progress_dict, task_id)
                    child_rel = os.path.join(current["rel_path"], safe_name(child["name"]))

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
                            "type": "file"
                        }
                        q.put(file_obj)

                        with _dl_lock:
                            if progress_dict and task_id:
                                info = progress_dict[task_id]
                                info["files_total"] = info.get("files_total", 0) + 1
                                info["message"] = f"Mapeando... ({info['files_total']} encontrados)"
                                progress_dict[task_id] = info

    except Exception as e:
        print(f"Erro no Mapper: {e}")
        with _dl_lock:
             if progress_dict and task_id:
                hist = progress_dict[task_id].get("history", [])
                hist.append(f"Erro no mapeamento: {str(e)}")
                progress_dict[task_id]["history"] = hist

def _concurrent_worker(creds, q, dest_root, used_rel_paths, progress_dict, task_id, filters, results_list):
    """
    CONSUMIDOR: Pega itens da fila e baixa.
    """
    while True:
        try:
            item = q.get(timeout=2)
        except queue.Empty:
            return 

        if item is None: # Poison pill
            break

        try:
            _worker_download_one(creds, item, dest_root, used_rel_paths, progress_dict, task_id, filters)
            with _dl_lock:
                results_list.append(item)
        except Exception as e:
            print(f"Erro worker: {e}")
        finally:
            q.task_done()

def execute_concurrent_download(creds, items, dest_root, progress_dict, task_id, filters):
    """
    Orquestra as threads de mapeamento e download simultâneos.
    """
    file_queue = queue.Queue()
    results_list = []
    used_rel_paths = set()

    # 1. Inicia Produtor (Mapper)
    mapper_thread = threading.Thread(
        target=_concurrent_mapper,
        args=(creds, items, file_queue, progress_dict, task_id, filters)
    )
    mapper_thread.start()

    # 2. Inicia Consumidores (Workers)
    workers = []
    for _ in range(MAX_DOWNLOAD_WORKERS):
        t = threading.Thread(
            target=_concurrent_worker,
            args=(creds, file_queue, dest_root, used_rel_paths, progress_dict, task_id, filters, results_list)
        )
        t.start()
        workers.append(t)

    # 3. Espera o mapeamento terminar
    mapper_thread.join()
    
    # 4. Espera a fila ser processada totalmente
    file_queue.join()

    # 5. Encerra workers
    for _ in range(MAX_DOWNLOAD_WORKERS):
        file_queue.put(None)

    for t in workers:
        t.join()

    return results_list


# =================================================================================
# FUNÇÕES PRINCIPAIS (INTERFACES)
# =================================================================================

def download_files_to_folder(
    creds,
    files_list: list[dict],
    dest_root: str,
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
    processing_mode: str = "sequential",
) -> None:
    # Usado pelo MODO SEQUENCIAL (lista já vem completa)
    if not files_list:
        return

    total = len(files_list)
    update_progress(task_id, {
        "phase": "baixando",
        "files_downloaded": 0,
        "files_total": total,
        "message": f"Iniciando download de {total} itens (Sequencial)...",
        "history": ["Iniciando downloads paralelos..."]
    })
    sync_task_to_db(task_id)

    used_rel_paths = set()
    changes_since_sync = 0

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
                if progress_dict and task_id and changes_since_sync >= 25:
                    sync_task_to_db(task_id)
                    changes_since_sync = 0
            except Exception as exc:
                if "Cancelado" in str(exc):
                    for f in futures: f.cancel()
                    sync_task_to_db(task_id)
                    raise exc

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
    processing_mode: str = "sequential",
) -> str:

    local_temp_base = os.path.join(os.getcwd(), "storage/temp_work")
    os.makedirs(local_temp_base, exist_ok=True)
    tmp_root = tempfile.mkdtemp(prefix="dl_", dir=local_temp_base)

    files_list_result = []

    try:
        check_status_pause_cancel(progress_dict, task_id)

        # === DECISÃO DE FLUXO (TURBO vs SEQUENCIAL) ===
        if processing_mode == "concurrent":
            # Modo Turbo: Mapper e Worker rodam juntos
            update_progress(task_id, {
                "phase": "mapeando", 
                "message": "MODO TURBO: Mapeando e Baixando simultaneamente...",
                "files_total": 0,
                "history": ["Iniciando Modo Concorrente (Turbo)..."]
            })
            files_list_result = execute_concurrent_download(
                creds, items, tmp_root, progress_dict, task_id, filters
            )
        else:
            # Modo Sequencial (Padrão): Mapeia tudo -> Baixa tudo
            service_main = build("drive", "v3", credentials=creds)
            files_list_result = build_files_list_for_items(
                service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id
            )

            if not files_list_result:
                raise Exception("Nenhum arquivo encontrado.")

            download_files_to_folder(
                creds, files_list_result, dest_root=tmp_root, progress_dict=progress_dict, task_id=task_id, filters=filters, processing_mode=processing_mode
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

            with zipfile.ZipFile(archive_path, "w", compression=comp, compresslevel=level, allowZip64=True) as zf:
                for f_info in files_list_result:
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
                for f_info in files_list_result:
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
    processing_mode: str = "sequential",
) -> None:

    dest_root_long = prepare_long_path(dest_root)
    os.makedirs(dest_root_long, exist_ok=True)

    check_status_pause_cancel(progress_dict, task_id)

    if processing_mode == "concurrent":
         update_progress(task_id, {
            "phase": "mapeando",
            "message": "Modo Espelho Turbo (Concorrente)...",
            "files_total": 0
        })
         execute_concurrent_download(
            creds, items, dest_root, progress_dict, task_id, filters
        )
    else:
        # Modo Sequencial
        service_main = build("drive", "v3", credentials=creds)
        files_list = build_files_list_for_items(
            service_main, items, creds=creds, filters=filters, progress_dict=progress_dict, task_id=task_id
        )
        if not files_list:
            raise Exception("Nenhum arquivo encontrado.")

        download_files_to_folder(
            creds, files_list, dest_root=dest_root, progress_dict=progress_dict, task_id=task_id, filters=filters, processing_mode=processing_mode
        )

    update_progress(task_id, {
        "phase": "concluido",
        "message": f"Espelho atualizado em: {dest_root}",
        "history": [f"Espelho concluído em: {dest_root}"]
    })
    sync_task_to_db(task_id)