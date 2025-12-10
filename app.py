from flask import Flask, request
import os
import logging

from config import (
    SECRET_KEY,
    SQLALCHEMY_DATABASE_URI,
    SQLALCHEMY_TRACK_MODIFICATIONS,
    TIMEZONE,
    BACKUP_RETENTION_MAX_FILES,
    BACKUP_RETENTION_MAX_DAYS,
    BACKUP_STORAGE_DIR,
    AUTH_TOKEN_FILE,
    LOG_ENABLED
)

from app.models import db
from app.utils.structured_logging import setup_logging, log_event

# IMPORTS DOS BLUEPRINTS
from app.blueprints.auth import auth_bp
from app.blueprints.drive import drive_bp
from app.blueprints.profile import profile_bp
from app.blueprints.admin import admin_bp
from app.blueprints.scheduler import scheduler_bp
from app.blueprints.health import health_bp
from app.blueprints.audit import audit_bp

from app.services.scheduler import init_scheduler


# --------------------------------------------------------------------
#  LOG DE ERROS DO FLASK
# --------------------------------------------------------------------
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



# --------------------------------------------------------------------
#  CREATE_APP — AQUI TUDO SE JUNTA
# --------------------------------------------------------------------
def create_app() -> Flask:

    # === TIMEZONE GLOBAL ===
    os.environ["TZ"] = TIMEZONE
    try:
        import time
        time.tzset()
    except Exception:
        pass

    app = Flask(__name__)
    app.secret_key = SECRET_KEY

    # ------------------------------
    # CONFIG DO BANCO
    # ------------------------------
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        SQLALCHEMY_DATABASE_URI + "?timeout=30&check_same_thread=False"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = SQLALCHEMY_TRACK_MODIFICATIONS

    # BACKUP CONFIGS
    app.config["BACKUP_RETENTION_MAX_FILES"] = BACKUP_RETENTION_MAX_FILES
    app.config["BACKUP_RETENTION_MAX_DAYS"] = BACKUP_RETENTION_MAX_DAYS
    app.config["BACKUP_STORAGE_DIR"] = BACKUP_STORAGE_DIR
    app.config["AUTH_TOKEN_FILE"] = AUTH_TOKEN_FILE
    app.config["TIMEZONE"] = TIMEZONE

    # Inicializa banco
    db.init_app(app)

    # ------------------------------
    # LOG ESTRUTURADO (ANTES DE TUDO)
    # ------------------------------
    setup_logging()

    log_event("app.start", severity="INFO")

    # Habilitar captura de logs do Flask
    redirect_flask_logs_to_structured()

    # Habilitar captura de erros automáticos
    register_flask_error_logging(app)

    # ------------------------------
    # BLUEPRINTS
    # ------------------------------
    app.register_blueprint(auth_bp)
    app.register_blueprint(drive_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(health_bp)
    app.register_blueprint(audit_bp)

    # ------------------------------
    # BANCO + SCHEDULER
    # ------------------------------
    with app.app_context():
        db.create_all()

        # Scheduler só roda no processo principal
        if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
            init_scheduler(app)

    return app


# --------------------------------------------------------------------
#  EXECUÇÃO DIRETA
# --------------------------------------------------------------------
if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)
