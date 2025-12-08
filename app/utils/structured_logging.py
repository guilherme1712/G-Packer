# structured_logging.py
import datetime as dt
import json
import logging
import logging.handlers
import os
import requests
import sys
import traceback

from config import LOG_ENABLED, LOG_EXTERNAL_ENABLED, LOG_EXTERNAL_URL
from app.services.storage import StorageService


# =================================================================
# FORMATO LARAVEL PARA LOGS
# =================================================================

class LaravelFormatter(logging.Formatter):
    """
    Formata logs no estilo do Laravel:
    [2025-01-18 13:55:22] local.INFO: Mensagem {"campo": "valor"}
    """

    def __init__(self, environment: str = "local"):
        super().__init__()
        self.environment = environment

    def format(self, record):
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname
        msg = record.getMessage()

        # Logs estruturados chegam como JSON → decodificar
        try:
            payload = json.loads(msg)
            event = payload.pop("event", None)
            severity = payload.pop("severity", level)

            # mensagem final
            # ex: "task.start"
            final_msg = event or "log"
            json_part = json.dumps(payload, ensure_ascii=False)

            formatted = f"[{ts}] {self.environment}.{severity}: {final_msg} {json_part}"

        except Exception:
            # fallback se não for JSON estruturado
            formatted = f"[{ts}] {self.environment}.{level}: {msg}"

        return formatted


# =================================================================
# CONFIGURAÇÃO DO LOGGER PRINCIPAL
# =================================================================

def setup_logging():
    # Se logs estiverem desativados → não cria logger nenhum
    if not LOG_ENABLED:
        return logging.getLogger("gpacker")  # retorna vazio

    logger = logging.getLogger("gpacker")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = LaravelFormatter(environment="local")

    # Console
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    # Arquivo local
    logs_dir = StorageService.logs_dir()
    log_file = os.path.join(logs_dir, "gpacker.log")

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Log externo
    if LOG_EXTERNAL_ENABLED:
        logger.external_url = LOG_EXTERNAL_URL

    return logger


# =================================================================
# LOG ESTRUTURADO (similar ao seu, mas compatível com laravel format)
# =================================================================

def log_event(event: str, severity: str = "INFO", **fields):

    # Não registra logs se estiver desativado
    if not LOG_ENABLED:
        return

    logger = logging.getLogger("gpacker")

    payload = {
        "ts": dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "event": event,
        "severity": severity.upper(),
        **fields,
    }

    json_payload = json.dumps(payload, ensure_ascii=False)

    logger.log(
        level=LEVEL_MAP.get(severity.upper(), logging.INFO),
        msg=json_payload
    )

    # Envio externo
    if LOG_EXTERNAL_ENABLED:
        try:
            requests.post(LOG_EXTERNAL_URL, json=payload, timeout=1)
        except Exception:
            pass


LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


__all__ = ["setup_logging", "log_event"]


# =================================================================
# ERROS GLOBAIS + THREADS
# =================================================================

def install_global_error_handlers():
    logger = logging.getLogger("gpacker")

    # Todas as exceções fora de try/except
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            return sys.__excepthook__(exc_type, exc_value, exc_traceback)

        trace = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

        log_event(
            "python.unhandled_exception",
            severity="ERROR",
            error=str(exc_value),
            exception_type=exc_type.__name__,
            traceback=trace
        )

        logger.error(trace)

    sys.excepthook = handle_exception

    # Exceções dentro de threads
    import threading

    def thread_exception_handler(args):
        trace = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))

        log_event(
            "thread.unhandled_exception",
            severity="ERROR",
            thread=args.thread.name,
            error=str(args.exc_value),
            exception_type=args.exc_type.__name__,
            traceback=trace
        )

        logger.error(trace)

    threading.excepthook = thread_exception_handler
