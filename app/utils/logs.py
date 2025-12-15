# --------------------------------------------------------------------
#  LOG DE ERROS DO FLASK
# --------------------------------------------------------------------
from app.utils.structured_logging import log_event
from config import LOG_ENABLED
from flask import request
import logging

def register_flask_error_logging(app):

    if not LOG_ENABLED:
        return

    @app.errorhandler(Exception)
    def handle_flask_exception(e):
        import traceback

        trace = traceback.format_exc()

        log_event(
            "flask.exception",
            severity="ERROR",
            path=request.path,
            method=request.method,
            error=str(e),
            traceback=trace
        )

        # API → JSON
        if request.path.startswith("/api"):
            return {"ok": False, "error": "internal_error"}, 500

        # Normal → HTML
        return (
            "<h1>Erro interno</h1><pre>{}</pre>".format(str(e)),
            500,
        )


# --------------------------------------------------------------------
#  REDIRECIONAR LOGS WERKZEUG PARA LOG PERSONALIZADO
# --------------------------------------------------------------------
def redirect_flask_logs_to_structured():

    if not LOG_ENABLED:
        return  # ignora por completo
    """
    Apenas captura erros do werkzeug (4xx, 5xx).
    Ignora logs normais de requisição.
    """
    flask_logger = logging.getLogger("werkzeug")

    class WerkzeugErrorFilter(logging.Filter):
        def filter(self, record):
            msg = record.getMessage()

            # só registra erros HTTP
            # exemplos:
            # "GET /rota HTTP/1.1" 500 -
            # "POST /rota HTTP/1.1" 404 -
            if " 500 " in msg or " 404 " in msg or " 400 " in msg:
                return True

            return False  # ignora 200/304/etc.

    class WerkzeugToJSON(logging.Handler):
        def emit(self, record):
            log_event(
                "http.error",
                severity=record.levelname,
                message=record.getMessage()
            )

    # limpa totalmente os handlers padrão do flask
    flask_logger.handlers.clear()

    # adiciona filtro + handler
    handler = WerkzeugToJSON()
    handler.addFilter(WerkzeugErrorFilter())
    flask_logger.addHandler(handler)


