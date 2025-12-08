# config.py
import os

# =========================================================
# 1. DIRETÓRIOS E AMBIENTE
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Permitir HTTP sem HTTPS (útil para desenvolvimento local)
os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

# Timezone padrão da aplicação
TIMEZONE = "America/Sao_Paulo"

# Diretório raiz para arquivos gerados pela app
STORAGE_DIR = os.path.join(BASE_DIR, "storage")

# Onde ficarão os arquivos de backup gerados (zips)
BACKUP_STORAGE_DIR = os.path.join(STORAGE_DIR, "backups")

# Onde ficarão arquivos de autenticação (token JSON do Google)
AUTH_STORAGE_DIR = os.path.join(STORAGE_DIR, "auth")
AUTH_TOKEN_FILE = os.path.join(AUTH_STORAGE_DIR, "token.json")


# =========================================================
# 2. FLASK E SEGURANÇA
# =========================================================
# Chave de sessão do Flask (Deve ser mantida segura em produção)
SECRET_KEY = "troque-esta-chave-por-algo-seguro"


# =========================================================
# 3. BANCO DE DADOS (SQLALCHEMY)
# =========================================================
# Caminho para o banco de dados SQLite
SQLALCHEMY_DATABASE_URI = f"sqlite:///{os.path.join(BASE_DIR, 'gpacker.db')}"

# Desativa o rastreamento de modificações para economizar memória
SQLALCHEMY_TRACK_MODIFICATIONS = False


# =========================================================
# 4. INTEGRAÇÃO GOOGLE (OAUTH & DRIVE)
# =========================================================
# Caminho para o arquivo JSON de credenciais do OAuth
CLIENT_SECRETS_FILE = os.path.join(
    BASE_DIR, "CREDENCIAIS", "G-Packer-credentials-app-web.json"
)

# Escopos de permissão solicitados ao usuário
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.activity.readonly",  # Ler histórico de atividades
    "https://www.googleapis.com/auth/drive.file",  # <<< ler/gravar arquivos criados pela app
]


# =========================================================
# 5. CONFIGURAÇÕES ESPECÍFICAS DA APP
# =========================================================
# Arquivo local onde os perfis de backup são salvos (JSON) – se você ainda usa
PROFILES_FILE = os.path.join(BASE_DIR, "backup_profiles.json")

# Quantidade máxima de backups a manter (0 ou None = ilimitado)
BACKUP_RETENTION_MAX_FILES = 12  # exemplo: 10

# Quantidade máxima de dias a manter (0 ou None = ilimitado)
BACKUP_RETENTION_MAX_DAYS = 30   # exemplo: 30

LOG_ENABLED = False  # False = desativa totalmente os logs
LOG_EXTERNAL_ENABLED = False  # envia logs para endpoint externo
LOG_EXTERNAL_URL = "http://meu-servico-de-logs/api/events"