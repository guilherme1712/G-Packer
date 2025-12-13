# services/drive_activity_service.py
from googleapiclient.discovery import build
from datetime import datetime

def get_action_name(action_detail):
    """Traduz o tipo de ação da API para português."""
    if 'create' in action_detail: return "CRIAR"
    if 'edit' in action_detail: return "EDITAR"
    if 'move' in action_detail: return "MOVER"
    if 'rename' in action_detail: return "RENOMEAR"
    if 'delete' in action_detail: return "EXCLUIR"
    if 'restore' in action_detail: return "RESTAURAR"
    if 'permissionChange' in action_detail: return "PERMISSÃO"
    if 'comment' in action_detail: return "COMENTÁRIO"
    if 'dlpChange' in action_detail: return "DLP"
    if 'reference' in action_detail: return "REFERÊNCIA"
    if 'settingsChange' in action_detail: return "CONFIGURAÇÃO"
    return "OUTRO"

def fetch_activity_log(creds, item_id):
    """
    Busca o histórico de atividades (v2) para um arquivo ou pasta.
    Retorna lista simplificada: [{date, action, actor}, ...]
    """
    try:
        service = build('driveactivity', 'v2', credentials=creds, cache_discovery=False)
        
        # O nome do item deve ser 'items/ID'
        item_name = f"items/{item_id}"
        
        # Busca atividades recentes
        response = service.activity().query(body={
            "itemName": item_name,
            "pageSize": 20
        }).execute()
        
        activities = response.get('activities', [])
        history = []
        
        for activity in activities:
            # 1. Data
            timestamp = activity.get('timestamp')
            if not timestamp and 'timeRange' in activity:
                timestamp = activity['timeRange'].get('endTime')
            
            date_str = "—"
            if timestamp:
                try:
                    # Remove o 'Z' e trata milissegundos se houver
                    ts_clean = timestamp.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(ts_clean)
                    date_str = dt.strftime("%d/%m/%Y %H:%M")
                except:
                    date_str = timestamp

            # 2. Ação
            primary_action = "Ação"
            if 'primaryActionDetail' in activity:
                primary_action = get_action_name(activity['primaryActionDetail'])

            # 3. Ator (Quem fez)
            actor_name = "Desconhecido"
            actors = activity.get('actors', [])
            if actors:
                user = actors[0].get('user', {})
                known_user = user.get('knownUser', {})
                actor_name = known_user.get('personName', 'Usuário')
                # Tenta pegar isCurrentUser se personName não ajudar
                if not actor_name and user.get('isCurrentUser'):
                    actor_name = "Você"
            
            history.append({
                "date": date_str,
                "action": primary_action,
                "actor": actor_name
            })
            
        return history

    except Exception as e:
        print(f"Erro ao buscar activity para {item_id}: {e}")
        return []