# controllers/profile_controller.py
from flask import Blueprint, request, jsonify

from services.auth_service import get_credentials
from services.profile_service import (
    load_backup_profiles,
    create_profile,
    get_profile,
    delete_profile,
)

profile_bp = Blueprint("profiles", __name__)


# -----------------------------------------------------------
# APIs para perfis de backup
# -----------------------------------------------------------
@profile_bp.route("/api/profiles", methods=["GET", "POST"])
def api_profiles():
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    if request.method == "GET":
        profiles = load_backup_profiles()
        return jsonify({"profiles": profiles})

    data = request.get_json(silent=True) or {}
    profile, error = create_profile(data)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    return jsonify({"ok": True, "profile": profile})


@profile_bp.route("/api/profiles/<profile_id>", methods=["GET", "DELETE"])
def api_profile_detail(profile_id):
    creds = get_credentials()
    if not creds:
        return jsonify({"error": "unauthorized"}), 401

    profile = get_profile(profile_id)
    if not profile:
        return jsonify({"error": "not_found"}), 404

    if request.method == "GET":
        return jsonify(profile)

    ok = delete_profile(profile_id)
    return jsonify({"ok": ok})
