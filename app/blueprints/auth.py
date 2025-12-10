# app/blueprints/auth.py
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
)
from app.services.auth import (
    get_credentials,
    build_flow,
    save_credentials,
)
from app.services.audit import AuditService  # <--- IMPORT

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/")
def index():
    creds = get_credentials()
    if creds:
        return redirect(url_for("admin.dashboard"))
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

@auth_bp.route("/logout")
def logout():
    # [AUDIT] Log antes de limpar a sessão
    user_email = AuditService.get_current_user_email()
    AuditService.log("LOGOUT", "Sistema", user_email=user_email)

    session.pop("google_auth_id", None)
    flash("Sessão encerrada com sucesso.")
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
        save_credentials(creds)

        # [AUDIT] Login realizado com sucesso
        # O email pode ser extraído se estiver disponível no objeto creds ou via serviço
        AuditService.log("LOGIN", "Google Auth", details="Autenticação OAuth2 completada")

        return redirect(url_for("admin.dashboard"))
    except Exception as e:
        flash(f"Erro ao concluir autenticação: {e}")
        return redirect(url_for("auth.index"))
