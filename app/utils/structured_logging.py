import datetime as dt
import json
import logging


LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def log_event(event: str, severity: str = "INFO", **fields) -> None:
    """
    Log estruturado em JSON.

    Exemplo de uso:
        log_event("health.database", status="ok", duration_ms=12.3)
    """
    logger = logging.getLogger("gpacker")

    level = LEVEL_MAP.get(severity.upper(), logging.INFO)

    payload = {
        "ts": dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "event": event,
        "severity": severity.upper(),
        **fields,
    }

    logger.log(level, json.dumps(payload, ensure_ascii=False))
