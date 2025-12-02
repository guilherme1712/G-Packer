# controllers/auth_controller.py
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
)

from services.auth_service import (
    get_credentials,
    build_flow,
    credentials_to_dict,
)

auth_bp = Blueprint("auth", __name__)


# -----------------------------------------------------------
# Rotas de autenticação / tela inicial
# -----------------------------------------------------------
@auth_bp.route("/")
def index():
    creds = get_credentials()
    if creds:
        # endpoint 'folders' dentro do blueprint 'drive'
        return redirect(url_for("drive.folders"))
    return render_template("index.html")


@auth_bp.route("/login")
def login():
    try:
        flow = build_flow()
        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
        )
        session["state"] = state
        return redirect(authorization_url)
    except Exception as e:
        flash(f"Erro ao iniciar autenticação: {e}")
        return redirect(url_for("auth.index"))


@auth_bp.route("/oauth2callback")
def oauth2callback():
    state = session.get("state")
    if not state:
        flash("Sessão de autenticação inválida. Tente novamente.")
        return redirect(url_for("auth.index"))

    try:
        flow = build_flow(state=state)
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        session["credentials"] = credentials_to_dict(creds)

        return redirect(url_for("drive.folders"))
    except Exception as e:
        flash(f"Erro ao concluir autenticação: {e}")
        return redirect(url_for("auth.index"))
