# app/blueprints/auth_service.py
import threading
import time
import uuid
from flask import (
    Blueprint,
    request,
    redirect,
    url_for,
    session,
    flash,
    current_app, render_template,
)
from app.services.auth_service import (
    get_credentials,
    build_flow,
    save_credentials,
)
from app.services.audit import AuditService
from app.models.db_instance import db
# Imports de modelo movidos para dentro das rotas/funções para evitar ciclos, se necessário
from app.models.task import TaskModel
from app.enum.task_type import TaskTypeEnum
from app.services.google.drive_cache_service import rebuild_full_cache

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/")
def index():
    creds = get_credentials()
    if creds:
        return redirect(url_for("admin.dashboard"))
    return render_template("index.html")  # Certifique-se de importar render_template se usar


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
    session.pop("google_auth_id", None)
    flash("Sessão encerrada.")
    return redirect(url_for("auth.index"))


# --- WRAPPER DA THREAD ---
def background_mapper_thread(app_obj, credentials, t_id):
    """
    Função isolada que roda em background.
    """
    # 1. DELAY CRÍTICO: Espera a rota de login liberar o banco SQLite
    print(f"--- [THREAD] Iniciando espera de 3s para Task {t_id} ---")
    time.sleep(3)

    # 2. Cria contexto da aplicação
    with app_obj.app_context():
        try:
            print(f"--- [THREAD] Conectando ao banco para Task {t_id} ---")

            # Executa a lógica pesada
            rebuild_full_cache(credentials, task_id=t_id)

        except Exception as e:
            print(f"!!! [THREAD FATAL ERROR] {e}")
            # Tenta salvar o erro no banco de emergência
            try:
                task = TaskModel.query.get(t_id)
                if task:
                    task.phase = "erro"
                    task.message = f"Erro fatal na thread: {str(e)}"
                    db.session.commit()
            except:
                pass


@auth_bp.route("/oauth2callback")
def oauth2callback():
    state = session.get("state")
    if not state:
        flash("Sessão inválida.")
        return redirect(url_for("auth.index"))

    try:
        flow = build_flow(state=state)
        flow.fetch_token(authorization_response=request.url)
        creds = flow.credentials
        save_credentials(creds)

        AuditService.log("LOGIN", "Google Auth", details="Sucesso")

        # 1. Cria a Task IMEDIATAMENTE e commita para garantir que o ID exista
        task_id = f"map-{uuid.uuid4().hex[:8]}"
        new_task = TaskModel(
            id=task_id,
            type=TaskTypeEnum.MAPPING,
            phase="iniciando",
            message="Aguardando liberação do banco...",
            files_found=0,
            files_total=0
        )
        db.session.add(new_task)
        db.session.commit()

        # 2. Captura o objeto APP real (thread-safe)
        app_real = current_app._get_current_object()

        # 3. Inicia a Thread passando o app real
        t = threading.Thread(
            target=background_mapper_thread,
            args=(app_real, creds, task_id),
            daemon=True
        )
        t.start()

        flash("Login realizado! O mapeamento iniciará em alguns segundos.")
        return redirect(url_for("admin.dashboard"))

    except Exception as e:
        flash(f"Erro no login: {e}")
        return redirect(url_for("auth.index"))