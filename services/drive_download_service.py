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

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials

from .drive_filters import safe_name, file_passes_filters
from .drive_tree_service import build_files_list_for_items
from .progress_service import sync_task_to_db

_dl_lock = threading.Lock()


def prepare_long_path(path: str) -> str:
    if os.name == "nt":
        path = os.path.abspath(path)
        if not path.startswith("\\\\?\\"):
            return f"\\\\?\\{path}"
    return path


def check_cancellation(progress_dict, task_id):
    if progress_dict and task_id:
        info = progress_dict.get(task_id, {})
        if info.get("canceled"):
            raise Exception("Cancelado pelo usuário")


def get_export_info(mime_type: str, file_name: str):
    if mime_type == "application/vnd.google-apps.document":
        return (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            file_name if file_name.lower().endswith(".docx") else file_name + ".docx",
        )
    if mime_type == "application/vnd.google-apps.spreadsheet":
        return (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            file_name if file_name.lower().endswith(".xlsx") else file_name + ".xlsx",
        )
    if mime_type == "application/vnd.google-apps.presentation":
        return (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            file_name if file_name.lower().endswith(".pptx") else file_name + ".pptx",
        )
    if mime_type == "application/vnd.google-apps.drawing":
        return (
            "image/png",
            file_name if file_name.lower().endswith(".png") else file_name + ".png",
        )
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
    """
    Worker que faz o download de UM arquivo.
    NÃO chama DB diretamente; apenas atualiza PROGRESS em memória.
    """
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    file_id = f_info["id"]
    mime = f_info.get("mimeType", "")
    original_name = f_info.get("name") or "arquivo"
    raw_rel_path = f_info.get("rel_path") or safe_name(original_name)
    rel_dir = os.path.dirname(raw_rel_path)

    download_name = original_name

    check_cancellation(progress_dict, task_id)

    if mime == "application/vnd.google-apps.shortcut":
        try:
            sc_meta = service.files().get(
                fileId=file_id, fields="shortcutDetails"
            ).execute()
            target = sc_meta.get("shortcutDetails", {}).get("targetId")
            if target:
                meta = service.files().get(
                    fileId=target, fields="id,name,mimeType,size"
                ).execute()
                file_id = meta["id"]
                mime = meta["mimeType"]
                download_name = meta["name"]
                if filters and not file_passes_filters(meta, filters):
                    return
            else:
                return
        except Exception:
            return

    request_dl = None
    if mime.startswith("application/vnd.google-apps."):
        export_mime, new_name = get_export_info(mime, download_name)
        if export_mime:
            download_name = new_name
            request_dl = service.files().export_media(
                fileId=file_id, mimeType=export_mime
            )
        else:
            return
    else:
        request_dl = service.files().get_media(fileId=file_id)

    final_rel_path = ""
    local_path = ""

    # Geração de caminho local único
    with _dl_lock:
        base_name_safe = safe_name(download_name)
        candidate_rel = (
            os.path.join(rel_dir, base_name_safe)
            if rel_dir
            else base_name_safe
        )

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

    try:
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request_dl)
        done = False
        while not done:
            check_cancellation(progress_dict, task_id)
            status, done = downloader.next_chunk()
    except Exception as e:
        if not fh.closed:
            fh.close()
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass

        if "Cancelado" in str(e):
            # deixa a exceção subir para a thread principal tratar
            raise e

        # log de erro apenas em memória
        with _dl_lock:
            if progress_dict and task_id:
                info = progress_dict[task_id]
                info["errors"] = info.get("errors", 0) + 1
                hist = info.get("history", [])
                hist.append(f"FALHA {download_name}: {str(e)}")
                info["history"] = hist[-100:]
                progress_dict[task_id] = info
        return
    finally:
        if not fh.closed:
            fh.close()

    # Guarda o caminho relativo efetivo
    f_info["local_rel_path"] = final_rel_path

    # Atualiza contadores em memória
    with _dl_lock:
        if progress_dict and task_id:
            info = progress_dict[task_id]
            dl_now = info.get("files_downloaded", 0) + 1
            info["files_downloaded"] = dl_now
            total = info.get("files_total", 0)
            info["message"] = f"Baixando arquivos ({dl_now}/{total})..."
            hist = info.get("history", [])
            hist.append(f"Baixado: {download_name}")
            info["history"] = hist[-50:]
            progress_dict[task_id] = info


def download_files_to_folder(
    creds,
    files_list: list[dict],
    dest_root: str,
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
) -> None:
    """
    Baixa todos os arquivos em paralelo para uma pasta local.

    - Workers NÃO falam com o DB.
    - Esta função roda dentro da thread de background (com app_context),
      então é ela que periodicamente chama sync_task_to_db().
    """
    if not files_list:
        return

    total = len(files_list)

    if progress_dict and task_id:
        with _dl_lock:
            info = progress_dict.get(task_id, {})
            info.update(
                {
                    "phase": "baixando",
                    "files_downloaded": 0,
                    "files_total": total,
                    "errors": info.get("errors", 0),
                    "message": f"Iniciando download de {total} itens...",
                }
            )
            hist = info.get("history", [])
            hist.append("Iniciando downloads paralelos...")
            info["history"] = hist[-100:]
            progress_dict[task_id] = info
        # flush inicial (fase "baixando")
        sync_task_to_db(task_id)

    used_rel_paths = set()
    MAX_WORKERS = 8
    changes_since_sync = 0  # controle de flush

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for f_info in files_list:
            fut = executor.submit(
                _worker_download_one,
                creds,
                f_info,
                dest_root,
                used_rel_paths,
                progress_dict,
                task_id,
                filters,
            )
            futures.append(fut)

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                # um arquivo concluído → provavelmente files_downloaded subiu
                changes_since_sync += 1
                if progress_dict and task_id and changes_since_sync >= 25:
                    sync_task_to_db(task_id)
                    changes_since_sync = 0
            except Exception as exc:
                if "Cancelado" in str(exc):
                    for f in futures:
                        f.cancel()
                    # flush de cancelamento
                    if progress_dict and task_id:
                        sync_task_to_db(task_id)
                    raise exc
                else:
                    print(f"Erro não tratado em thread de download: {exc}")

    # flush final do estágio de download
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
) -> str:
    """
    Orquestra o mapeamento, download e compactação em um único bundle.
    É chamada pela thread de background que já está em app_context.
    """
    service_main = build("drive", "v3", credentials=creds)

    files_list = build_files_list_for_items(
        service_main,
        items,
        creds=creds,
        filters=filters,
        progress_dict=progress_dict,
        task_id=task_id,
    )

    if not files_list:
        raise Exception("Nenhum arquivo encontrado para download.")

    check_cancellation(progress_dict, task_id)

    local_temp_base = os.path.join(os.getcwd(), "storage/temp_work")
    os.makedirs(local_temp_base, exist_ok=True)

    tmp_root = tempfile.mkdtemp(prefix="dl_", dir=local_temp_base)

    try:
        # 1) Downloads em paralelo
        download_files_to_folder(
            creds,
            files_list,
            dest_root=tmp_root,
            progress_dict=progress_dict,
            task_id=task_id,
            filters=filters,
        )

        check_cancellation(progress_dict, task_id)

        # 2) Passa para fase de compactação
        if progress_dict and task_id:
            with _dl_lock:
                info = progress_dict.get(task_id, {})
                info.update(
                    {"phase": "compactando", "message": "Compactando arquivos..."}
                )
                hist = info.get("history", [])
                hist.append("Iniciando compactação...")
                info["history"] = hist[-100:]
                progress_dict[task_id] = info
            sync_task_to_db(task_id)

        out_dir = tempfile.mkdtemp(prefix="out_", dir=local_temp_base)

        if not base_name:
            base_name = "backup_drive"
        base_name = safe_name(base_name)

        if archive_format == "zip":
            archive_path = os.path.join(out_dir, f"{base_name}.zip")
            comp = zipfile.ZIP_DEFLATED
            level = 6
            if compression_level == "fast":
                level = 1
            elif compression_level == "max":
                level = 9

            try:
                zf = zipfile.ZipFile(
                    archive_path, "w", compression=comp, compresslevel=level
                )
            except TypeError:
                # Python mais antigo sem compresslevel
                zf = zipfile.ZipFile(archive_path, "w", compression=comp)

            with zf:
                for f_info in files_list:
                    check_cancellation(progress_dict, task_id)
                    rel = f_info.get("local_rel_path")
                    if not rel:
                        continue

                    src = os.path.join(tmp_root, rel)
                    src_long = prepare_long_path(src)

                    if os.path.exists(src_long):
                        zf.write(src_long, arcname=rel)
        else:
            archive_path = os.path.join(out_dir, f"{base_name}.tar.gz")
            with tarfile.open(archive_path, "w:gz") as tf:
                for f_info in files_list:
                    check_cancellation(progress_dict, task_id)
                    rel = f_info.get("local_rel_path")
                    if not rel:
                        continue

                    src = os.path.join(tmp_root, rel)
                    src_long = prepare_long_path(src)

                    if os.path.exists(src_long):
                        tf.add(src_long, arcname=rel)

    except Exception as e:
        try:
            shutil.rmtree(prepare_long_path(tmp_root), ignore_errors=True)
        except Exception:
            pass
        # Em caso de erro grande, faz flush do estado de erro
        if progress_dict and task_id:
            sync_task_to_db(task_id)
        raise e

    # Limpa pasta temporária de downloads
    try:
        shutil.rmtree(prepare_long_path(tmp_root), ignore_errors=True)
    except Exception:
        pass

    # Finaliza
    if progress_dict and task_id:
        with _dl_lock:
            info = progress_dict.get(task_id, {})
            info.update(
                {"phase": "concluido", "message": "Pacote gerado com sucesso!"}
            )
            hist = info.get("history", [])
            hist.append("Processo finalizado com sucesso.")
            info["history"] = hist[-100:]
            progress_dict[task_id] = info
        sync_task_to_db(task_id)

    return archive_path


def mirror_items_to_local(
    creds: Credentials,
    items: list,
    dest_root: str,
    progress_dict=None,
    task_id: str | None = None,
    filters: dict | None = None,
) -> None:
    """
    Espelho local (sem compactar).
    Também é chamado pela thread de background com app_context.
    """
    service_main = build("drive", "v3", credentials=creds)

    files_list = build_files_list_for_items(
        service_main,
        items,
        creds=creds,
        filters=filters,
        progress_dict=progress_dict,
        task_id=task_id,
    )
    if not files_list:
        raise Exception("Nenhum arquivo encontrado.")

    check_cancellation(progress_dict, task_id)

    dest_root_long = prepare_long_path(dest_root)
    os.makedirs(dest_root_long, exist_ok=True)

    download_files_to_folder(
        creds,
        files_list,
        dest_root=dest_root,
        progress_dict=progress_dict,
        task_id=task_id,
        filters=filters,
    )

    if progress_dict and task_id:
        with _dl_lock:
            info = progress_dict.get(task_id, {})
            info.update(
                {
                    "phase": "concluido",
                    "message": f"Espelho atualizado em: {dest_root}",
                }
            )
            hist = info.get("history", [])
            hist.append(f"Espelho concluído em: {dest_root}")
            info["history"] = hist[-100:]
            progress_dict[task_id] = info
        sync_task_to_db(task_id)
