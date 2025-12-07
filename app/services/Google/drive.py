# services/drive_filters.py
import re
from datetime import datetime, timezone

def safe_name(name: str) -> str:
    """Remove caracteres problemáticos para uso em caminhos/arquivos."""
    return re.sub(r'[\\/:*?"<>|]', "_", name)


def classify_mime(mime: str) -> str:
    """Classifica MIME em grupos lógicos usados nos filtros."""
    mime = mime or ""

    if mime == "application/pdf":
        return "pdf"

    if mime in {
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.google-apps.spreadsheet",
    }:
        return "sheets"

    if mime in {
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.google-apps.document",
    }:
        return "docs"

    if mime in {
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.google-apps.presentation",
    }:
        return "docs"

    if mime.startswith("image/"):
        return "images"

    if mime.startswith("video/"):
        return "videos"

    return "others"


def parse_rfc3339(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def extract_size_bytes(meta: dict) -> int:
    size_str = meta.get("size")
    try:
        return int(size_str)
    except (TypeError, ValueError):
        return 0


def file_passes_filters(meta: dict, filters: dict | None) -> bool:
    """Aplica filtros de tipo, data e tamanho em um arquivo do Drive."""
    if not filters:
        return True

    mime = meta.get("mimeType", "")
    group = classify_mime(mime)
    allowed_groups = filters.get("groups")
    if allowed_groups and group not in allowed_groups:
        return False

    created_after = filters.get("created_after")
    if created_after:
        ct = parse_rfc3339(meta.get("createdTime"))
        if not ct or ct < created_after:
            return False

    modified_after = filters.get("modified_after")
    if modified_after:
        mt = parse_rfc3339(meta.get("modifiedTime"))
        if not mt or mt < modified_after:
            return False

    max_size = filters.get("max_size_bytes")
    if max_size is not None:
        size_bytes = extract_size_bytes(meta)
        if size_bytes and size_bytes > max_size:
            return False

    return True


def parse_date_input(raw: str | None) -> datetime | None:
    """Converte 'YYYY-MM-DD' em datetime com tz UTC, ou None se vazio/inválido."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        d = datetime.strptime(raw, "%Y-%m-%d")
        return d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def build_filters_from_form(form) -> dict:
    """
    Lê request.form e monta o dicionário de filtros usado nos services
    de Drive (tipos, datas, tamanho).
    """
    groups = set()
    if form.get("type_pdf"):
        groups.add("pdf")
    if form.get("type_docs"):
        groups.add("docs")
    if form.get("type_sheets"):
        groups.add("sheets")
    if form.get("type_images"):
        groups.add("images")
    if form.get("type_videos"):
        groups.add("videos")
    if form.get("type_others"):
        groups.add("others")

    # Se nenhum tipo marcado, considera todos
    if not groups:
        groups = {"pdf", "docs", "sheets", "images", "videos", "others"}

    created_after = parse_date_input(form.get("created_after"))
    modified_after = parse_date_input(form.get("modified_after"))

    max_size_mb_raw = (form.get("max_size_mb") or "").strip()
    max_size_bytes = None
    if max_size_mb_raw:
        try:
            max_size_bytes = int(float(max_size_mb_raw) * 1024 * 1024)
        except Exception:
            max_size_bytes = None

    return {
        "groups": groups,
        "created_after": created_after,
        "modified_after": modified_after,
        "max_size_bytes": max_size_bytes,
    }
