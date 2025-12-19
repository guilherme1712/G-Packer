# app/blueprints/cleaner.py
from flask import Blueprint, render_template, request, jsonify, session
from app.services.tools.deduplication_service import DeduplicationService
# Certifique-se de importar o helper de credenciais corretamente
# (Ajuste o import abaixo se sua função get_credentials estiver em outro lugar, ex: auth_service)
from app.services.auth_service import get_credentials 

cleaner_bp = Blueprint('cleaner', __name__, url_prefix='/cleaner')

@cleaner_bp.route('/')
def index():
    return render_template('tools/cleaner_dashboard.html')

@cleaner_bp.route('/api/stats')
def api_stats():
    """
    Retorna dados JSON para os gráficos e a QUOTA DO DRIVE.
    """
    try:
        # 1. Busca estatísticas do cache local (Gráfico)
        stats = DeduplicationService.get_storage_analysis()

        # 2. Busca Quota real na API do Google
        quota = {}
        creds = get_credentials()
        quota = DeduplicationService.get_drive_quota(creds)

        return jsonify({
            "ok": True, 
            "data": stats,
            "quota": quota  # Adicionado aqui
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@cleaner_bp.route('/api/duplicates')
def api_duplicates():
    try:
        dupes = DeduplicationService.find_duplicates()
        return jsonify({"ok": True, "data": dupes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@cleaner_bp.route('/api/large-files')
def api_large_files():
    try:
        limit = request.args.get('limit', 50, type=int)
        files = DeduplicationService.find_large_files(limit=limit)
        return jsonify({"ok": True, "data": files})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@cleaner_bp.route('/api/trash', methods=['POST'])
def api_trash_bulk():
    data = request.json
    file_ids = data.get('file_ids', [])
    
    if not file_ids:
        return jsonify({"ok": False, "error": "Nenhum arquivo selecionado"}), 400

    creds = get_credentials(session.get('user_id')) 
    if not creds:
        return jsonify({"ok": False, "error": "Credenciais não encontradas"}), 401

    try:
        result = DeduplicationService.bulk_trash_files(creds, file_ids)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500