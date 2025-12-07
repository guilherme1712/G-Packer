# services/auth_service.py
import os
import json

from flask import session, url_for, has_request_context
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from models import db, GoogleAuthModel

from config import CLIENT_SECRETS_FILE, SCOPES

# # Pasta/arquivo onde o token será salvo para uso pelos jobs agendados
# AUTH_DIR = os.path.join(os.getcwd(), "storage", "auth")
# TOKEN_PATH = os.path.join(AUTH_DIR, "token.json")


def credentials_to_dict(c: Credentials) -> dict:
    """Converte Credentials em dict serializável para guardar na sessão/arquivo."""
    return {
        "token": c.token,
        "refresh_token": c.refresh_token,
        "token_uri": c.token_uri,
        "client_id": c.client_id,
        "client_secret": c.client_secret,
        "scopes": c.scopes,
    }


def save_credentials(creds: Credentials) -> None:
    """
    Agora:
    - Apenas persiste no banco (GoogleAuthModel) e guarda só o ID do registro na sessão.
    """
    data = credentials_to_dict(creds)

    # tenta extrair o e-mail se o id_token estiver presente
    email = None
    id_token = getattr(creds, "id_token", None)
    if isinstance(id_token, dict):
        email = id_token.get("email")

    auth = _save_credentials_to_db(data, email=email)

    # na sessão guardamos só o ID da linha, NÃO o token
    if has_request_context():
        session["google_auth_id"] = auth.id

def _save_credentials_to_db(data: dict, email: str | None = None) -> GoogleAuthModel:
    """
    Salva/atualiza um registro global de credenciais Google.
    Aqui eu assumo 1 só conta Google para o app inteiro.
    """
    # tenta pegar o registro ativo mais recente
    auth = (GoogleAuthModel.query
            .filter_by(active=True)
            .order_by(GoogleAuthModel.updated_at.desc())
            .first())

    if not auth:
        auth = GoogleAuthModel()
        db.session.add(auth)

    if email:
        auth.email = email

    auth.token_json = json.dumps(data)
    auth.active = True
    db.session.commit()
    return auth

def _load_credentials_from_db() -> Credentials | None:
    """
    Tenta:
    1) Usar o ID salvo na sessão (se tiver);
    2) Senão, pega o registro ativo mais recente (login “global”).
    """
    auth = None

    # 1) Se tiver request, tenta pelo ID da sessão
    if has_request_context():
        auth_id = session.get("google_auth_id")
        if auth_id:
            auth = GoogleAuthModel.query.get(auth_id)

    # 2) Fallback: pega qualquer ativo mais recente
    if not auth:
        auth = (GoogleAuthModel.query
                .filter_by(active=True)
                .order_by(GoogleAuthModel.updated_at.desc())
                .first())

    if not auth:
        return None

    data = json.loads(auth.token_json)
    # usa from_authorized_user_info porque temos um dict serializável
    return Credentials.from_authorized_user_info(data, data.get("scopes"))


def get_credentials() -> Credentials | None:
    """
    Versão nova:
    - Primeiro tenta buscar no banco;
    - Se não houver nada, tenta migrar do token.json legado;
    - Não lê mais da sessão (além do ID).
    """
    creds = _load_credentials_from_db()
    if creds:
        return creds

def build_flow(state: str | None = None) -> Flow:
    """Cria o Flow de OAuth com o redirect correto."""
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for("auth.oauth2callback", _external=True),
        state=state,
    )
