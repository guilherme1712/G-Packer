# controllers/scheduler_controller.py
from flask import Blueprint, render_template, request, jsonify, current_app
import json

from models import db, ScheduledTaskModel
from services.scheduler_service import reload_jobs

scheduler_bp = Blueprint("scheduler", __name__)

@scheduler_bp.route("/scheduler")
def index():
    tasks = ScheduledTaskModel.query.all()
    return render_template("scheduler.html", tasks=tasks)

@scheduler_bp.route("/scheduler/create", methods=["POST"])
def create_task():
    data = request.json
    name = data.get("name")
    items = data.get("items")
    frequency = data.get("frequency")
    run_time = data.get("run_time", "03:00")
    zip_name = data.get("zip_name", "backup_auto")

    if not name or not items or not frequency:
        return jsonify({"ok": False, "error": "Dados incompletos"}), 400

    new_task = ScheduledTaskModel(
        name=name,
        items_json=json.dumps(items),
        zip_name=zip_name,
        frequency=frequency,
        run_time=run_time,
        active=True,
        last_status="Aguardando..."
    )


    db.session.add(new_task)
    try:
        db.session.commit()

        # Atualiza o scheduler em tempo real
        reload_jobs(current_app._get_current_object())

        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

@scheduler_bp.route("/scheduler/delete/<int:task_id>", methods=["POST"])
def delete_task(task_id):
    task = ScheduledTaskModel.query.get(task_id)
    if task:
        db.session.delete(task)
        db.session.commit()
        reload_jobs(current_app._get_current_object())
    return jsonify({"ok": True})

@scheduler_bp.route("/scheduler/toggle/<int:task_id>", methods=["POST"])
def toggle_task(task_id):
    task = ScheduledTaskModel.query.get(task_id)
    if task:
        task.active = not task.active
        db.session.commit()
        reload_jobs(current_app._get_current_object())
        return jsonify({"ok": True, "active": task.active})

    return jsonify({"ok": False, "error": "Task não encontrada"}), 404
