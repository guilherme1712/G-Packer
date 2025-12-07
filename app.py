from flask import Flask
import os

from config import (
    SECRET_KEY,
    SQLALCHEMY_DATABASE_URI,
    SQLALCHEMY_TRACK_MODIFICATIONS,
    TIMEZONE,
    BACKUP_RETENTION_MAX_FILES,
    BACKUP_RETENTION_MAX_DAYS,
)
from app.models import db

# Importação dos controllers (Blueprints)
from app.blueprints.auth import auth_bp
from app.blueprints.drive import drive_bp
from app.blueprints.profile import profile_bp
from app.blueprints.admin import admin_bp
from app.blueprints.scheduler import scheduler_bp
from app.services.scheduler import init_scheduler
from app.blueprints.health import health_bp

def create_app() -> Flask:
    # === TIMEZONE GLOBAL ===
    os.environ["TZ"] = TIMEZONE
    try:
        import time
        time.tzset()  # funciona em Linux / macOS
    except Exception:
        pass

    app = Flask(__name__)
    app.secret_key = SECRET_KEY

    # Configuração do Banco de Dados
    app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = SQLALCHEMY_TRACK_MODIFICATIONS
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        SQLALCHEMY_DATABASE_URI + "?timeout=30&check_same_thread=False"
    )
    app.config["TIMEZONE"] = TIMEZONE

        # >>> NOVO: Configs globais de retenção de backups
    app.config["BACKUP_RETENTION_MAX_FILES"] = BACKUP_RETENTION_MAX_FILES
    app.config["BACKUP_RETENTION_MAX_DAYS"] = BACKUP_RETENTION_MAX_DAYS
    

    # Inicializa o Banco com a App
    db.init_app(app)

    # Registra blueprints (controllers)
    app.register_blueprint(auth_bp)
    app.register_blueprint(drive_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(health_bp)

    # Cria as tabelas do banco se não existirem
    with app.app_context():
        db.create_all()
        if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
            init_scheduler(app)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)
