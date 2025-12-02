# services/auth_service.py
from flask import session, url_for
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

from config import CLIENT_SECRETS_FILE, SCOPES


def credentials_to_dict(c: Credentials) -> dict:
    """Converte Credentials em dict serializável para guardar na sessão."""
    return {
        "token": c.token,
        "refresh_token": c.refresh_token,
        "token_uri": c.token_uri,
        "client_id": c.client_id,
        "client_secret": c.client_secret,
        "scopes": c.scopes,
    }


def get_credentials() -> Credentials | None:
    """Recupera as credenciais do usuário a partir da sessão do Flask."""
    if "credentials" not in session:
        return None
    data = session["credentials"]
    return Credentials(**data)


def build_flow(state: str | None = None) -> Flow:
    """Cria o Flow de OAuth com o redirect correto."""
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for("auth.oauth2callback", _external=True),
        state=state,
    )
