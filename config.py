# config.py
import os

# Diretório base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Credenciais do OAuth do Google
CLIENT_SECRETS_FILE = os.path.join(
    BASE_DIR, "CREDENCIAIS", "G-Packer-credentials-app-web.json"
)

# Escopos usados na autorização
# ADICIONADO: drive.activity.readonly para ler histórico
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.activity.readonly",
]

# Arquivo local onde os perfis de backup são salvos
PROFILES_FILE = os.path.join(BASE_DIR, "backup_profiles.json")

# Chave de sessão do Flask
SECRET_KEY = "troque-esta-chave-por-algo-seguro"

# Permitir HTTP sem HTTPS (para desenvolvimento local)
os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

# Configuração do SQLite
SQLALCHEMY_DATABASE_URI = f"sqlite:///{os.path.join(BASE_DIR, 'gpacker.db')}"
SQLALCHEMY_TRACK_MODIFICATIONS = False