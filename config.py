# # config.py
import os

# # Diretório base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# # Credenciais do OAuth do Google
CLIENT_SECRETS_FILE = os.path.join(
    BASE_DIR, "CREDENCIAIS", "G-Packer-credentials-app-web.json"
)

# # Escopos usados na autorização
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# # Arquivo local onde os perfis de backup são salvos
PROFILES_FILE = os.path.join(BASE_DIR, "backup_profiles.json")

# # Chave de sessão do Flask
SECRET_KEY = "troque-esta-chave-por-algo-seguro"

# # Caminho do Banco de Dados SQLite
# DB_FILE = os.path.join(BASE_DIR, "app_data.db")

# # Permitir HTTP sem HTTPS (para desenvolvimento local)
os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

# config.py

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Arquivo de credenciais do Google (mantenha como está)
# CLIENT_SECRETS_FILE = os.path.join(BASE_DIR, "G-Packer-credentials-app-web.json")
# SCOPES = [
#     "https://www.googleapis.com/auth/drive.readonly",
#     "openid",
#     "https://www.googleapis.com/auth/userinfo.email",
#     "https://www.googleapis.com/auth/userinfo.profile",
# ]

# Configuração do SQLite
SQLALCHEMY_DATABASE_URI = f"sqlite:///{os.path.join(BASE_DIR, 'gpacker.db')}"
SQLALCHEMY_TRACK_MODIFICATIONS = False
