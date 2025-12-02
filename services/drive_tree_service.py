# services/drive_tree_service.py
import os
import threading
import concurrent.futures
from googleapiclient.discovery import build

from .drive_filters import (
    safe_name,
    file_passes_filters,
    extract_size_bytes,
)

# Lock global para garantir que logs e listas não corrompam com múltiplas threads
_lock = threading.Lock()

def list_children(service, parent_id: str, include_files: bool):
    """
    Lista filhos diretos de um item (usado apenas na UI da árvore, mantém síncrono).
    """
    items = []
    page_token = None

    while True:
        try:
            results = service.files().list(
                q=f"'{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=1000,
                spaces="drive",
            ).execute()
        except Exception as e:
            # Em caso de erro na listagem da UI, retorna o que tem ou loga
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
    service = build("drive", "v3", credentials=creds)
    return list_children(service, parent_id, include_files)


def get_file_metadata(creds, file_id: str) -> dict:
    service = build("drive", "v3", credentials=creds)
    meta = service.files().get(
        fileId=file_id,
        fields=(
            "id,name,mimeType,size,createdTime,modifiedTime,"
            "webViewLink,iconLink,thumbnailLink"
        ),
    ).execute()
    return meta


def check_cancellation(progress_dict, task_id):
    """Lê flag de cancelamento. Thread-safe por natureza (leitura de dict)."""
    if progress_dict and task_id:
        info = progress_dict.get(task_id, {})
        if info.get("canceled"):
            raise Exception("Cancelado pelo usuário")


def _worker_process_folder(
    creds,
    folder_id,
    base_path,
    filters,
    progress_dict,
    task_id
):
    """
    Worker que processa UMA pasta.
    """
    # IMPORTANTE: Cria um serviço usando as credenciais recebidas.
    # Não acessa sessão nem request aqui.
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    local_files = []
    subfolders_to_scan = [] # Tuplas (id, path)

    page_token = None

    while True:
        check_cancellation(progress_dict, task_id)

        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)",
                pageToken=page_token,
                pageSize=1000,
                spaces="drive",
            ).execute()
        except Exception as e:
            with _lock:
                if progress_dict and task_id:
                    info = progress_dict.get(task_id, {})
                    hist = info.get("history", [])
                    hist.append(f"ERRO ao ler pasta {base_path}: {e}")
                    info["history"] = hist[-100:]
                    progress_dict[task_id] = info
            return [], []

        for f in results.get("files", []):
            check_cancellation(progress_dict, task_id)

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
    creds=None,  # <--- NOVA ALTERAÇÃO: Recebe creds explicitamente
    filters: dict | None = None,
    progress_dict=None,
    task_id: str | None = None,
):
    """
    Mapeia arquivos usando ThreadPoolExecutor para paralelismo.
    """
    # CORREÇÃO: Usa as credenciais passadas explicitamente.
    # Se não passadas, tenta extrair do service (mas sem fallback para session)
    if not creds:
        if hasattr(service_main, 'credentials'):
            creds = service_main.credentials

    # Se ainda assim creds for None, vai dar erro no build() dentro do worker,
    # mas pelo menos não crasha tentando acessar request/session.
    if not creds:
        raise ValueError("Credenciais não fornecidas para a thread de mapeamento.")

    all_files_list = []

    if progress_dict is not None and task_id is not None:
        progress_dict[task_id].update({
            "phase": "mapeando",
            "files_found": 0,
            "files_total": 0,
            "bytes_found": 0,
            "message": "Iniciando mapeamento paralelo...",
            "history": ["Iniciando mapeamento (Multi-thread)..."]
        })

    initial_folders = []

    for it in items:
        it_id = it.get("id")
        it_name = it.get("name") or "item"
        it_type = it.get("type", "file")
        root_prefix = safe_name(it_name)

        if it_type == "folder":
            initial_folders.append((it_id, root_prefix))
        else:
            try:
                meta = service_main.files().get(
                    fileId=it_id,
                    fields="id, name, mimeType, createdTime, modifiedTime, size",
                ).execute()

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

                    with _lock:
                        if progress_dict and task_id:
                            info = progress_dict[task_id]
                            info["files_found"] = info.get("files_found", 0) + 1
                            info["bytes_found"] = info.get("bytes_found", 0) + size_bytes
                            hist = info.get("history", [])
                            hist.append(f"Mapeado: {rel_path}")
                            info["history"] = hist[-100:]
                            progress_dict[task_id] = info
            except Exception as e:
                print(f"Erro ao pegar arquivo raiz {it_name}: {e}")

    # PARALELISMO
    MAX_WORKERS = 8

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_folder = {}

        for fid, fpath in initial_folders:
            # Passa creds explicitamente para o worker
            f = executor.submit(_worker_process_folder, creds, fid, fpath, filters, progress_dict, task_id)
            future_to_folder[f] = fpath

        while future_to_folder:
            check_cancellation(progress_dict, task_id)

            done, not_done = concurrent.futures.wait(
                future_to_folder.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED
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
                                info["files_found"] = info.get("files_found", 0) + len(found_files)
                                total_bytes = sum(x['size_bytes'] for x in found_files)
                                info["bytes_found"] = info.get("bytes_found", 0) + total_bytes
                                hist = info.get("history", [])
                                for ff in found_files:
                                    hist.append(f"Mapeado: {ff['rel_path']}")
                                info["history"] = hist[-100:]
                                progress_dict[task_id] = info

                    for sub_id, sub_path in found_subfolders:
                        new_future = executor.submit(_worker_process_folder, creds, sub_id, sub_path, filters, progress_dict, task_id)
                        future_to_folder[new_future] = sub_path

                except Exception as exc:
                    err_msg = str(exc)
                    if "Cancelado" in err_msg:
                        for pending in future_to_folder:
                            pending.cancel()
                        raise exc

                    print(f"Erro na thread de pasta {fpath_orig}: {exc}")
                    with _lock:
                         if progress_dict and task_id:
                            info = progress_dict[task_id]
                            hist = info.get("history", [])
                            hist.append(f"ERRO ao processar pasta {fpath_orig}: {exc}")
                            info["history"] = hist[-100:]
                            progress_dict[task_id] = info

    if progress_dict is not None and task_id is not None:
        total_bytes = sum(f.get("size_bytes", 0) for f in all_files_list)
        with _lock:
            info = progress_dict.get(task_id, {})
            info["files_total"] = len(all_files_list)
            info["bytes_found"] = total_bytes
            mb = total_bytes / (1024 * 1024) if total_bytes else 0
            info["message"] = (
                f"Mapeamento concluído (Paralelo). Total: {len(all_files_list)} arquivos (~{mb:.1f} MB)."
            )
            hist = info.get("history", [])
            hist.append("Mapeamento finalizado.")
            info["history"] = hist[-100:]
            progress_dict[task_id] = info

    return all_files_list
