# services/auth_service.py
import os
import json

from flask import session, url_for, has_request_context
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

from config import CLIENT_SECRETS_FILE, SCOPES

# Pasta/arquivo onde o token será salvo para uso pelos jobs agendados
AUTH_DIR = os.path.join(os.getcwd(), "storage", "auth")
TOKEN_PATH = os.path.join(AUTH_DIR, "token.json")


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
    Salva as credenciais:
    - na session (quando existir request HTTP)
    - em disco (token.json) para uso pelo APScheduler.
    """
    data = credentials_to_dict(creds)

    # Guarda na sessão se estivermos dentro de um request
    if has_request_context():
        session["credentials"] = data

    # Garante pasta e salva em arquivo
    os.makedirs(AUTH_DIR, exist_ok=True)
    with open(TOKEN_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)


def get_credentials() -> Credentials | None:
    """
    Recupera credenciais:
    - Se houver request e a sessão tiver 'credentials', usa esse valor.
    - Caso contrário, tenta carregar do token.json (para jobs agendados).
    """
    # 1) Estamos numa request HTTP → usa sessão
    if has_request_context() and "credentials" in session:
        data = session["credentials"]
        return Credentials(**data)

    # 2) Fora de request (ex.: APScheduler) → tenta carregar do arquivo
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        # usa from_authorized_user_info porque já temos o dict serializável
        return Credentials.from_authorized_user_info(data, data.get("scopes"))

    # 3) Nada encontrado
    return None


def build_flow(state: str | None = None) -> Flow:
    """Cria o Flow de OAuth com o redirect correto."""
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for("auth.oauth2callback", _external=True),
        state=state,
    )
