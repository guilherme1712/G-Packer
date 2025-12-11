# services/drive_tree_service.py
import os
import time
import threading
import concurrent.futures
import random
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .drive import safe_name, file_passes_filters, extract_size_bytes
from app.services.progress import sync_task_to_db, update_progress
from app.services.worker_manager import WorkerManager  # <--- IMPORT NOVO

# Lock para operações globais (como atualizar progresso compartilhado)
_lock = threading.Lock()

# Armazenamento local da thread para reutilizar a conexão com a API
# Isso evita recriar o objeto 'service' milhares de vezes.
_thread_local = threading.local()

# Configurações
RETRY_LIMIT = 8
# MAX_MAPPING_WORKERS foi removido pois agora usamos o WorkerManager

def get_thread_safe_service(creds):
    """
    Retorna uma instância do serviço Drive reutilizável para a thread atual.
    Isso aumenta drasticamente a velocidade do mapeamento.
    """
    if not hasattr(_thread_local, "service"):
        # Cria o serviço apenas se esta thread ainda não tiver um
        _thread_local.service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _thread_local.service


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
    """Decorador para tentar novamente em caso de Rate Limit (403/429/5xx)."""
    def wrapper(*args, **kwargs):
        delay = 1
        for i in range(RETRY_LIMIT):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                # 403/429: Rate Limit, 5xx: Server Error
                if e.resp.status in [403, 429, 500, 502, 503]:
                    if i == RETRY_LIMIT - 1:
                        raise e
                    # Jitter para evitar que todas as threads tentem no mesmo milissegundo
                    sleep_time = delay + random.uniform(0, 1)
                    # print(f"Rate Limit ({e.resp.status}). Thread esperando {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    delay *= 2  # Backoff exponencial
                else:
                    raise e
            except Exception as e:
                # Erros de conexão socket (reset, timeout)
                if i == RETRY_LIMIT - 1:
                    raise e
                time.sleep(1)
    return wrapper


@exponential_backoff
def safe_list_execute(request_obj):
    return request_obj.execute()


# ---------------------------------------------------------------------------
# FUNÇÕES DE SERVIÇO (SINGLE-THREADED USAGE)
# ---------------------------------------------------------------------------

def list_children(service, folder_id, include_files: bool = False) -> list[dict]:
    """
    Lista filhos diretos de uma pasta no Drive.

    Agora retorna também:
      - mimeType
      - modified_time (string ISO)
    """
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "files(id, name, mimeType, size, modifiedTime), nextPageToken"

    items: list[dict] = []
    page_token = None

    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields=fields,
                pageToken=page_token,
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        for f in resp.get("files", []):
            mime = f.get("mimeType")
            modified = f.get("modifiedTime")
            is_folder = mime == "application/vnd.google-apps.folder"

            if is_folder:
                items.append(
                    {
                        "id": f["id"],
                        "name": f["name"],
                        "type": "folder",
                        "mimeType": mime,
                        "modified_time": modified,
                    }
                )
            elif include_files:
                size = int(f.get("size") or 0)
                items.append(
                    {
                        "id": f["id"],
                        "name": f["name"],
                        "type": "file",
                        "size_bytes": size,
                        "mimeType": mime,
                        "modified_time": modified,
                    }
                )

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # Pastas primeiro, depois arquivos, em ordem alfabética
    items.sort(key=lambda x: (x["type"] != "folder", x["name"].lower()))
    return items


def get_children(creds, parent_id: str, include_files: bool):
    service = get_thread_safe_service(creds) # Usa a versão otimizada
    return list_children(service, parent_id, include_files)


def get_file_metadata(creds, file_id: str) -> dict:
    service = get_thread_safe_service(creds)
    req = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,size,createdTime,modifiedTime,webViewLink,iconLink,thumbnailLink",
    )
    meta = safe_list_execute(req)
    return meta


def get_ancestors_path(creds, target_id: str) -> list:
    service = get_thread_safe_service(creds)
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


# ---------------------------------------------------------------------------
# WORKER OTIMIZADO PARA MAPEAMENTO
# ---------------------------------------------------------------------------

def _worker_process_folder(
    creds,
    folder_id,
    base_path,
    filters,
    progress_dict,
    task_id,
):
    """
    Worker executado por thread.
    Usa 'get_thread_safe_service' para não recriar conexão SSL a cada chamada.
    """
    service = get_thread_safe_service(creds)

    local_files = []
    subfolders_to_scan = []
    page_token = None

    while True:
        # Verifica pausa a cada página para ser responsivo
        check_status_pause_cancel(progress_dict, task_id)

        try:
            # Solicita apenas os campos essenciais para performance
            req = service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)",
                pageToken=page_token,
                pageSize=1000, # Máximo permitido
                spaces="drive",
            )
            results = safe_list_execute(req)
        except Exception as e:
            update_progress(task_id, {"history": [f"ERRO ao ler pasta {base_path}: {e}"]})
            # Em caso de erro na pasta, retorna o que achou até agora
            return local_files, subfolders_to_scan

        files = results.get("files", [])

        # Processamento em memória é rápido, o gargalo é a API
        for f in files:
            file_id = f["id"]
            name = f["name"]
            mime = f["mimeType"]

            if mime == "application/vnd.google-apps.folder":
                # Achou pasta: prepara para a próxima iteração do "Divide and Conquer"
                new_base = os.path.join(base_path, safe_name(name))
                subfolders_to_scan.append((file_id, new_base))
            else:
                # Achou arquivo: verifica filtro e adiciona
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
                    "modifiedTime": f.get("modifiedTime"),
                    "createdTime": f.get("createdTime"),
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

    # Inicializa thread_local para a thread principal também, se necessário
    if not hasattr(_thread_local, "service"):
        _thread_local.service = service_main

    if progress_dict is not None and task_id is not None:
        update_progress(task_id, {
            "phase": "mapeando",
            "files_found": 0,
            "files_total": 0,
            "bytes_found": 0,
            "bytes_downloaded": 0,
            "message": f"Iniciando mapeamento Turbo...", # Removida a contagem fixa de workers
            "history": ["Iniciando mapeamento otimizado..."]
        })
        sync_task_to_db(task_id)

    initial_folders = []
    changes_since_sync = 0

    # 1. Processa itens raiz (Nível 0)
    for it in items:
        check_status_pause_cancel(progress_dict, task_id)

        it_id = it.get("id")
        it_name = it.get("name") or "item"
        it_type = it.get("type", "file")
        root_prefix = safe_name(it_name)

        if it_type == "folder":
            initial_folders.append((it_id, root_prefix))
        else:
            # Processa arquivos raiz
            try:
                # Usa método seguro que já tem retry
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
                        "modifiedTime": meta.get("modifiedTime"),
                        "createdTime": meta.get("createdTime"),
                    }
                    all_files_list.append(obj)

                    with _lock:
                        if progress_dict and task_id:
                            info = progress_dict[task_id]
                            info["files_found"] = info.get("files_found", 0) + 1
                            info["bytes_found"] = info.get("bytes_found", 0) + size_bytes
                            info["message"] = f"Mapeando: {info['files_found']} itens..."
                            progress_dict[task_id] = info
                            changes_since_sync += 1

                    if changes_since_sync >= 50:
                        sync_task_to_db(task_id)
                        changes_since_sync = 0

            except Exception as e:
                print(f"Erro item raiz {it_name}: {e}")

    # 2. Processa Subpastas (Dividir para Conquistar com ThreadPool GLOBAL)
    # SUBSTITUIÇÃO CRÍTICA: Usamos o Executor Global do WorkerManager
    executor = WorkerManager.get_download_executor()
    
    # Mantemos controle apenas das tarefas deste job específico
    future_to_folder = {}

    # "Semeia" o executor global com as pastas iniciais
    for fid, fpath in initial_folders:
        f = executor.submit(
            _worker_process_folder, creds, fid, fpath, filters, progress_dict, task_id
        )
        future_to_folder[f] = fpath

    # Loop principal de consumo
    while future_to_folder:
        check_status_pause_cancel(progress_dict, task_id)

        # Espera a primeira tarefa deste job terminar
        # Nota: 'wait' funciona perfeitamente com um conjunto de futures, mesmo em um pool compartilhado
        done, _ = concurrent.futures.wait(
            future_to_folder.keys(),
            return_when=concurrent.futures.FIRST_COMPLETED,
        )

        for future in done:
            fpath_orig = future_to_folder.pop(future)

            try:
                found_files, found_subfolders = future.result()

                # 1. Adiciona Arquivos encontrados (RESULTADO)
                if found_files:
                    with _lock:
                        all_files_list.extend(found_files)

                        if progress_dict and task_id:
                            info = progress_dict[task_id]
                            count_now = info.get("files_found", 0) + len(found_files)
                            info["files_found"] = count_now

                            total_bytes = sum(x["size_bytes"] for x in found_files)
                            info["bytes_found"] = info.get("bytes_found", 0) + total_bytes
                            info["message"] = f"Mapeando: {count_now} itens encontrados..."

                            # Log inteligente
                            hist = info.get("history", [])
                            if len(found_files) < 3:
                                for ff in found_files:
                                    hist.append(f"Mapeado: {ff['rel_path']}")
                            else:
                                hist.append(f"Mapeados +{len(found_files)} arquivos em {fpath_orig}")
                            info["history"] = hist

                            progress_dict[task_id] = info
                            changes_since_sync += 1

                    if progress_dict and task_id and changes_since_sync >= 100:
                        sync_task_to_db(task_id)
                        changes_since_sync = 0

                # 2. Divide para Conquistar: Submete novas pastas ao Executor Global
                # Se o pool estiver cheio, isso vai para a fila do pool global, respeitando o limite
                for sub_id, sub_path in found_subfolders:
                    new_future = executor.submit(
                        _worker_process_folder, creds, sub_id, sub_path, filters, progress_dict, task_id
                    )
                    future_to_folder[new_future] = sub_path

            except Exception as exc:
                err_msg = str(exc)
                if "Cancelado" in err_msg:
                    # Cancela apenas os futuros deste job
                    for pending in future_to_folder:
                        pending.cancel()
                    raise exc

                print(f"Erro no worker de mapeamento para {fpath_orig}: {exc}")
                update_progress(task_id, {"history": [f"ERRO pasta {fpath_orig}: {exc}"]})

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

def calculate_selection_stats(creds, items):
    """
    Calcula estatísticas com LIMITES DE SEGURANÇA.
    Se demorar muito ou tiver muitos itens, retorna uma estimativa parcial.
    """
    service = get_thread_safe_service(creds)

    # LIMITES DE SEGURANÇA (Para não travar o modal)
    MAX_SCAN_TIME = 3.0    # Máximo 3 segundos escaneando
    MAX_SCAN_ITEMS = 3000  # Máximo 3000 itens para contar

    start_time = time.time()

    stats = {
        "files_count": 0,
        "folders_count": 0,
        "total_size_bytes": 0,
        "preview_files": [],
        "is_partial": False  # Flag para avisar o front que parou no meio
    }

    stack = []

    # 1. Processa seleção inicial
    for item in items:
        if item.get("type") == "folder":
            stats["folders_count"] += 1
            stack.append(item["id"])
        else:
            stats["files_count"] += 1
            s = int(item.get("size_bytes", 0))
            if s == 0 and item.get("size"): s = int(item["size"])
            stats["total_size_bytes"] += s
            if len(stats["preview_files"]) < 5:
                stats["preview_files"].append(item["name"])

    # 2. Varredura com verificação de limites
    while stack:
        # VERIFICAÇÃO DE SEGURANÇA (SAÍDA RÁPIDA)
        elapsed = time.time() - start_time
        count_total = stats["files_count"] + stats["folders_count"]

        if elapsed > MAX_SCAN_TIME or count_total > MAX_SCAN_ITEMS:
            stats["is_partial"] = True
            break  # <--- PARA O LOOP AQUI

        parent_id = stack.pop(0)
        page_token = None

        while True:
            # Verifica limites dentro da paginação também
            if (time.time() - start_time) > MAX_SCAN_TIME:
                stats["is_partial"] = True
                break

            try:
                # Pede APENAS o necessário
                results = service.files().list(
                    q=f"'{parent_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType, size)",
                    pageToken=page_token,
                    pageSize=1000
                ).execute()

                files = results.get("files", [])

                for f in files:
                    if f["mimeType"] == "application/vnd.google-apps.folder":
                        stats["folders_count"] += 1
                        stack.append(f["id"])
                    else:
                        stats["files_count"] += 1
                        stats["total_size_bytes"] += int(f.get("size", 0))

                        if len(stats["preview_files"]) < 5:
                            stats["preview_files"].append(f["name"])

                page_token = results.get("nextPageToken")
                if not page_token:
                    break
            except Exception as e:
                print(f"Erro scan leve: {e}")
                break

        if stats["is_partial"]:
            break

    return stats