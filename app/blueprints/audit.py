# app/blueprints/audit.py
from flask import (
    Blueprint,
    render_template,
    jsonify,
    redirect,
    url_for,
    request,
)
from dateutil.parser import parse
from app.services.audit import AuditService
from app.services.auth_service import get_credentials

audit_bp = Blueprint("audit", __name__)

@audit_bp.route("/audit")
def index():
    creds = get_credentials()
    if not creds:
        return redirect(url_for('auth.login'))
    return render_template("audit_logs.html")

@audit_bp.route("/api/audit/logs")
def api_logs():
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    action = request.args.get("action")

    start_date = parse(start_str) if start_str else None
    end_date = parse(end_str) if end_str else None

    logs = AuditService.fetch_logs(start_date, end_date, action, limit=500)
    return jsonify([l.to_dict() for l in logs])
