# app/blueprints/graph_viz.py
from flask import Blueprint, render_template, jsonify, request
from app.models.drive_cache import DriveItemCacheModel
from app.services.auth import get_credentials

graph_bp = Blueprint("graph_viz", __name__, url_prefix="/graph")

@graph_bp.route("/view")
def view_graph():
    """Renderiza a página HTML com o canvas do grafo."""
    return render_template("graph_view.html")

@graph_bp.route("/api/data/<folder_id>")
def api_graph_data(folder_id):
    """
    Retorna nós e arestas dos filhos de uma pasta específica.
    Usa o Cache Local para ser instantâneo.
    """
    # Verificação básica de auth (opcional, mas recomendado)
    if not get_credentials():
        return jsonify({"error": "Unauthorized"}), 401

    # Busca no banco local
    items = DriveItemCacheModel.query.filter_by(parent_id=folder_id, trashed=False).all()

    nodes = []
    edges = []

    for item in items:
        # Lógica visual
        is_folder = item.is_folder

        # Cor: Pastas Amarelas, Arquivos Azuis
        color = "#FFD700" if is_folder else "#97C2FC"
        shape = "dot"

        # Tamanho: Pastas fixas, arquivos variam levemente pelo tamanho (logarítmico simulado)
        size = 25 if is_folder else 15
        if not is_folder and item.size_bytes > 1000000: # > 1MB
            size = 20

        node = {
            "id": item.drive_id,
            "label": item.name,
            "group": "folder" if is_folder else "file",
            "title": f"Tipo: {item.mime_type}\nTamanho: {item.size_bytes} bytes", # Tooltip
            "shape": shape,
            "color": color,
            "size": size
        }
        nodes.append(node)

        # Cria a conexão (Aresta) da pasta pai -> item atual
        edge = {
            "from": folder_id,
            "to": item.drive_id,
            "length": 150 # Distância da linha
        }
        edges.append(edge)

    return jsonify({"nodes": nodes, "edges": edges})
