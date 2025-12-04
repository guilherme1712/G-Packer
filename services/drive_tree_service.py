# services/drive_tree_service.py
import os
import time
import threading
import concurrent.futures
import random
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .drive_filters import safe_name, file_passes_filters, extract_size_bytes
from .progress_service import sync_task_to_db, update_progress


_lock = threading.Lock()

# Configuração de alta performance
MAX_MAPPING_WORKERS = 20  # Threads de mapeamento
RETRY_LIMIT = 5           # Tentativas em caso de erro 403/500


def check_status_pause_cancel(progress_dict, task_id):
    """
    Verifica se a tarefa foi cancelada ou pausada.
    """
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


def exponential_backoff(func):
    """Decorador para tentar novamente em caso de Rate Limit (403/429)."""
    def wrapper(*args, **kwargs):
        delay = 1
        for i in range(RETRY_LIMIT):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                # Erros de cota ou temporários do servidor
                if e.resp.status in [403, 429, 500, 502, 503]:
                    if i == RETRY_LIMIT - 1:
                        raise e
                    sleep_time = delay + random.uniform(0, 1)
                    print(f"Rate Limit/Erro API ({e.resp.status}). Tentando em {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    delay *= 2  # Backoff exponencial
                else:
                    raise e
            except Exception as e:
                raise e
    return wrapper


@exponential_backoff
def safe_list_execute(request_obj):
    return request_obj.execute()


def list_children(service, parent_id: str, include_files: bool):
    items = []
    page_token = None
    while True:
        try:
            req = service.files().list(
                q=f"'{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=1000,
                spaces="drive",
            )
            results = safe_list_execute(req)
        except Exception as e:
            print(f"Erro ao listar filhos de {parent_id}: {e}")
            break

        for f in results.get("files", []):
            mime = f["mimeType"]
            if mime == "application/vnd.google-apps.folder":
                items.append({"id": f["id"], "name": f["name"], "type": "folder"})
            else:
                if include_files:
                    items.append({"id": f["id"], "name": f["name"], "type": "file"})

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    items.sort(key=lambda x: x["name"].lower())
    return items


def get_children(creds, parent_id: str, include_files: bool):
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return list_children(service, parent_id, include_files)


def get_file_metadata(creds, file_id: str) -> dict:
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    req = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,size,createdTime,modifiedTime,webViewLink,iconLink,thumbnailLink",
    )
    meta = safe_list_execute(req)
    return meta


def get_ancestors_path(creds, target_id: str) -> list:
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    path_ids = []
    current_id = target_id

    for _ in range(50):
        if not current_id: break
        path_ids.insert(0, current_id)
        if current_id == 'root': break

        try:
            req = service.files().get(fileId=current_id, fields="parents")
            f = safe_list_execute(req)
            parents = f.get('parents')
            if parents: current_id = parents[0]
            else: break
        except Exception as e:
            print(f"Erro ao resolver caminho para {current_id}: {e}")
            break

    return path_ids


def _worker_process_folder(
    creds,
    folder_id,
    base_path,
    filters,
    progress_dict,
    task_id,
):
    # Cria serviço local para a thread para thread-safety e performance
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    local_files = []
    subfolders_to_scan = []
    page_token = None

    while True:
        check_status_pause_cancel(progress_dict, task_id)

        try:
            req = service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)",
                pageToken=page_token,
                pageSize=1000,
                spaces="drive",
            )
            results = safe_list_execute(req)
        except Exception as e:
            update_progress(task_id, {"history": [f"ERRO ao ler pasta {base_path}: {e}"]})
            return [], []

        for f in results.get("files", []):
            check_status_pause_cancel(progress_dict, task_id)

            file_id = f["id"]
            name = f["name"]
            mime = f["mimeType"]

            if mime == "application/vnd.google-apps.folder":
                new_base = os.path.join(base_path, safe_name(name))
                subfolders_to_scan.append((file_id, new_base))
            else:
                if not file_passes_filters(f, filters):
                    continue

                rel_path = os.path.join(base_path, safe_name(name))
                size_bytes = extract_size_bytes(f)

                local_files.append({
                    "id": file_id,
                    "name": name,
                    "mimeType": mime,
                    "rel_path": rel_path,
                    "size_bytes": size_bytes,
                })

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    return local_files, subfolders_to_scan


def build_files_list_for_items(
    service_main,
    items: list,
    creds=None,
    filters: dict | None = None,
    progress_dict=None,
    task_id: str | None = None,
):
    if not creds:
        if hasattr(service_main, "credentials"):
            creds = service_main.credentials
    if not creds:
        raise ValueError("Credenciais não fornecidas.")

    all_files_list: list[dict] = []
    changes_since_sync = 0

    if progress_dict is not None and task_id is not None:
        update_progress(task_id, {
            "phase": "mapeando",
            "files_found": 0,
            "files_total": 0,
            "bytes_found": 0,
            "message": f"Iniciando mapeamento ultra-rápido ({MAX_MAPPING_WORKERS} threads)...",
            "history": ["Iniciando mapeamento otimizado..."]
        })
        sync_task_to_db(task_id)

    initial_folders = []

    # 1. Processa itens raiz
    for it in items:
        check_status_pause_cancel(progress_dict, task_id)

        it_id = it.get("id")
        it_name = it.get("name") or "item"
        it_type = it.get("type", "file")
        root_prefix = safe_name(it_name)

        if it_type == "folder":
            initial_folders.append((it_id, root_prefix))
        else:
            try:
                req = service_main.files().get(
                    fileId=it_id,
                    fields="id, name, mimeType, createdTime, modifiedTime, size",
                )
                meta = safe_list_execute(req)

                if file_passes_filters(meta, filters):
                    fname = meta["name"]
                    mime = meta["mimeType"]
                    size_bytes = extract_size_bytes(meta)
                    rel_path = os.path.join(root_prefix, safe_name(fname))

                    obj = {
                        "id": it_id,
                        "name": fname,
                        "mimeType": mime,
                        "rel_path": rel_path,
                        "size_bytes": size_bytes,
                    }
                    all_files_list.append(obj)

                    # Atualização inicial
                    with _lock:
                        if progress_dict and task_id:
                            info = progress_dict[task_id]
                            info["files_found"] = info.get("files_found", 0) + 1
                            info["bytes_found"] = info.get("bytes_found", 0) + size_bytes
                            # Feedback visual imediato
                            info["message"] = f"Mapeando: {info['files_found']} itens encontrados..."
                            progress_dict[task_id] = info
                            changes_since_sync += 1

                    if progress_dict and task_id and changes_since_sync >= 50:
                        sync_task_to_db(task_id)
                        changes_since_sync = 0

            except Exception as e:
                print(f"Erro item raiz {it_name}: {e}")

    # 2. Processa Subpastas (ThreadPool Otimizado)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_MAPPING_WORKERS) as executor:
        future_to_folder = {}

        # Submete iniciais
        for fid, fpath in initial_folders:
            f = executor.submit(
                _worker_process_folder, creds, fid, fpath, filters, progress_dict, task_id
            )
            future_to_folder[f] = fpath

        while future_to_folder:
            check_status_pause_cancel(progress_dict, task_id)

            done, _ = concurrent.futures.wait(
                future_to_folder.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            for future in done:
                fpath_orig = future_to_folder.pop(future)
                try:
                    found_files, found_subfolders = future.result()

                    if found_files:
                        with _lock:
                            all_files_list.extend(found_files)
                            if progress_dict and task_id:
                                info = progress_dict[task_id]
                                count_now = info.get("files_found", 0) + len(found_files)
                                info["files_found"] = count_now
                                
                                total_bytes = sum(x["size_bytes"] for x in found_files)
                                info["bytes_found"] = info.get("bytes_found", 0) + total_bytes

                                # -- AQUI ESTÁ A ATUALIZAÇÃO DO CONTADOR VISUAL NA MENSAGEM --
                                info["message"] = f"Mapeando: {count_now} itens encontrados..."
                                
                                # Histórico
                                hist = info.get("history", [])
                                if len(found_files) < 5:
                                    for ff in found_files:
                                        hist.append(f"Mapeado: {ff['rel_path']}")
                                else:
                                    hist.append(f"Mapeados +{len(found_files)} arquivos em {fpath_orig}")
                                info["history"] = hist
                                
                                progress_dict[task_id] = info
                                changes_since_sync += 1

                        if progress_dict and task_id and changes_since_sync >= 50:
                            sync_task_to_db(task_id)
                            changes_since_sync = 0

                    # Adiciona novas subpastas para processamento IMEDIATO
                    for sub_id, sub_path in found_subfolders:
                        new_future = executor.submit(
                            _worker_process_folder, creds, sub_id, sub_path, filters, progress_dict, task_id
                        )
                        future_to_folder[new_future] = sub_path

                except Exception as exc:
                    err_msg = str(exc)
                    if "Cancelado" in err_msg:
                        for pending in future_to_folder:
                            pending.cancel()
                        raise exc

                    update_progress(task_id, {"history": [f"ERRO processar pasta {fpath_orig}: {exc}"]})
                    changes_since_sync += 1

    # Finalização
    if progress_dict is not None and task_id is not None:
        total_bytes = sum(f.get("size_bytes", 0) for f in all_files_list)
        mb = total_bytes / (1024 * 1024) if total_bytes else 0
        update_progress(task_id, {
            "files_total": len(all_files_list),
            "bytes_found": total_bytes,
            "message": f"Mapeamento concluído. {len(all_files_list)} arquivos (~{mb:.1f} MB).",
            "history": ["Mapeamento finalizado."]
        })
        sync_task_to_db(task_id)

    return all_files_list