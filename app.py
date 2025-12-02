from flask import Flask

from config import SECRET_KEY, SQLALCHEMY_DATABASE_URI, SQLALCHEMY_TRACK_MODIFICATIONS
from models import db

# Importação dos controllers (Blueprints)
from controllers.auth_controller import auth_bp
from controllers.drive_controller import drive_bp
from controllers.profile_controller import profile_bp
from controllers.admin_controller import admin_bp  # <--- Novo import

def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = SECRET_KEY

    # Configuração do Banco de Dados
    app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = SQLALCHEMY_TRACK_MODIFICATIONS

    # Inicializa o Banco com a App
    db.init_app(app)

    # Registra blueprints (controllers)
    app.register_blueprint(auth_bp)
    app.register_blueprint(drive_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(admin_bp)  # <--- Registrando o novo blueprint

    # Cria as tabelas do banco se não existirem
    with app.app_context():
        db.create_all()

    return app

if __name__ == "__main__":
    app = create_app()
    # Host 0.0.0.0 permite acesso externo (útil se rodar em container ou VM)
    app.run(debug=True, host="0.0.0.0", port=5000)
