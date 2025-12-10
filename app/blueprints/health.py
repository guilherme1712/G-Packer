# controllers/health.py
from flask import (
    Blueprint,
    render_template,
    jsonify,
    redirect,
    url_for,
)
from app.services.auth import get_credentials
from app.services.healthcheck import run_health_checks

health_bp = Blueprint("health", __name__)


@health_bp.route("/health", methods=["GET"])
def health_json():
    """
    Endpoint para uso por monitoramento / automação.
    Retorna JSON com status + métricas do sistema.
    """
    data = run_health_checks()
    http_status = 200 if data["status"] != "error" else 503
    return jsonify(data), http_status


@health_bp.route("/status", methods=["GET"])
def status_page():
    creds = get_credentials()
    if not creds:
        return redirect(url_for('auth.login'))
    data = run_health_checks()
    return render_template("health_status.html", health=data)
