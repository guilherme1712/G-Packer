# app/services/auth_service.py
import json

from flask import session, url_for, has_request_context
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from app.models import db, GoogleAuthModel

from config import CLIENT_SECRETS_FILE, SCOPES


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
    Busca informações completas do usuário na API e persiste no banco.
    """
    data = credentials_to_dict(creds)

    user_data = {}

    try:
        # Usa as credenciais para buscar o perfil do usuário na API oauth2/v2
        service = build('oauth2', 'v2', credentials=creds)
        user_info = service.userinfo().get().execute()

        # Extrai os dados disponíveis graças aos escopos 'email' e 'profile'
        user_data['email'] = user_info.get('email')
        user_data['name'] = user_info.get('name')  # Nome completo
        user_data['picture'] = user_info.get('picture')  # URL da foto

    except Exception as e:
        print(f"Erro ao buscar informações do perfil Google: {e}")

    # Salva no banco passando os dados recuperados
    auth = _save_credentials_to_db(data, user_data=user_data)

    # na sessão guardamos só o ID da linha
    if has_request_context():
        session["google_auth_id"] = auth.id


def _save_credentials_to_db(data: dict, user_data: dict = None) -> GoogleAuthModel:
    """
    Salva/atualiza um registro global de credenciais Google com nome e foto.
    """
    if user_data is None:
        user_data = {}

    # tenta pegar o registro ativo mais recente
    auth = (GoogleAuthModel.query
            .filter_by(active=True)
            .order_by(GoogleAuthModel.updated_at.desc())
            .first())

    if not auth:
        auth = GoogleAuthModel()
        db.session.add(auth)

    # Atualiza campos se vierem da API
    if user_data.get('email'):
        auth.email = user_data['email']

    if user_data.get('name'):
        auth.name = user_data['name']

    if user_data.get('picture'):
        auth.picture = user_data['picture']

    auth.token_json = json.dumps(data)
    auth.active = True
    db.session.commit()
    return auth


def _load_credentials_from_db() -> Credentials | None:
    """
    Carrega credenciais do banco (lógica mantida).
    """
    auth = None

    if has_request_context():
        auth_id = session.get("google_auth_id")
        if auth_id:
            auth = GoogleAuthModel.query.get(auth_id)

    if not auth:
        auth = (GoogleAuthModel.query
                .filter_by(active=True)
                .order_by(GoogleAuthModel.updated_at.desc())
                .first())

    if not auth:
        return None

    data = json.loads(auth.token_json)
    return Credentials.from_authorized_user_info(data, data.get("scopes"))


def get_credentials() -> Credentials | None:
    creds = _load_credentials_from_db()
    if creds:
        return creds


def build_flow(state: str | None = None) -> Flow:
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for("auth.oauth2callback", _external=True),
        state=state,
    )