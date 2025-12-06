# controllers/scheduler_controller.py
from flask import Blueprint, render_template, request, jsonify, current_app
import json

from models import db, ScheduledTaskModel, BackupProfile
from services.scheduler_service import reload_jobs

scheduler_bp = Blueprint("scheduler", __name__)

@scheduler_bp.route("/scheduler")
def index():
    tasks = ScheduledTaskModel.query.all()
    return render_template("scheduler.html", tasks=tasks)

@scheduler_bp.route("/scheduler/create", methods=["POST"])
def create_task():
    data = request.json or {}

    name = data.get("name")
    items = data.get("items")
    frequency = data.get("frequency")
    run_time = data.get("run_time", "03:00")
    zip_name = data.get("zip_name", "backup_auto")

    source = data.get("source", "items")  # 'items' | 'profile'
    profile_id = data.get("profile_id")

    if not name or not frequency:
        return jsonify({"ok": False, "error": "Nome e frequência são obrigatórios."}), 400

    # ----------------------------------------------
    # Validação por modo de origem (itens ou perfil)
    # ----------------------------------------------
    profile_id_value = None

    if source == "profile":
        if not profile_id:
            return jsonify({"ok": False, "error": "Perfil de backup não informado."}), 400

        try:
            pid_int = int(profile_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Perfil de backup inválido."}), 400

        prof = BackupProfile.query.get(pid_int)
        if not prof:
            return jsonify({"ok": False, "error": "Perfil de backup não encontrado."}), 404

        profile_id_value = pid_int
        # items_json ainda é obrigatório no modelo -> guarda lista vazia
        items_json = "[]"

    else:
        # modo padrão: usa itens fixos
        if not items:
            return jsonify({"ok": False, "error": "Nenhum item selecionado para o agendamento."}), 400
        items_json = json.dumps(items)

    new_task = ScheduledTaskModel(
        name=name,
        items_json=items_json,
        zip_name=zip_name,
        frequency=frequency,
        run_time=run_time,
        active=True,
        last_status="Aguardando...",
        profile_id=profile_id_value,
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
