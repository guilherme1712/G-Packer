# import logging

from flask import Flask
import os
from app.blueprints.tools.conversor import tools_bp
from app.utils.logs import redirect_flask_logs_to_structured, register_flask_error_logging
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
from app.blueprints.upload import upload_bp
from app.blueprints.tools.packer import packer_bp
from app.blueprints.graph_viz import graph_bp

from app.services.scheduler import init_scheduler

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
    app.config['MAX_CONTENT_LENGTH'] = None
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
    app.register_blueprint(upload_bp)
    app.register_blueprint(tools_bp)
    app.register_blueprint(packer_bp)
    app.register_blueprint(graph_bp)

    # ------------------------------
    # BANCO + SCHEDULER
    # ------------------------------
    with app.app_context():
        db.create_all()

        # Scheduler só roda no processo principal
        if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
            init_scheduler(app)

    return app

