from flask import Blueprint, jsonify, render_template

from app.services.healthcheck import run_health_checks

health_bp = Blueprint("health", __name__)


@health_bp.route("/health", methods=["GET"])
def health_json():
    """
    Endpoint para uso por monitoramento / automação.
    Retorna JSON simples com status de cada parte do sistema.
    """
    data = run_health_checks()
    http_status = 200 if data["status"] != "error" else 503
    return jsonify(data), http_status


@health_bp.route("/status", methods=["GET"])
def status_page():
    """
    Página HTML bonitinha mostrando o status.
    """
    data = run_health_checks()
    return render_template("health_status.html", health=data)
