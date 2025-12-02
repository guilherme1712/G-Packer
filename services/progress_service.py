import threading
import copy
from models import db, TaskModel

# Cache em memória
PROGRESS: dict[str, dict] = {}

# LOCK: O segredo para resolver conflitos de threads.
# Qualquer leitura ou escrita no PROGRESS deve ser feita dentro deste lock.
_progress_lock = threading.Lock()

def init_download_task(task_id: str) -> dict:
    """
    Inicializa a task na memória e cria o registro no Banco de Dados.
    Thread-safe.
    """
    initial_state = {
        "phase": "iniciando",
        "mapped_folders": 0,
        "files_found": 0,
        "files_total": 0,
        "files_downloaded": 0,
        "bytes_found": 0,
        "errors": 0,
        "message": "Iniciando processo...",
        "history": ["Tarefa criada."],
        "canceled": False
    }

    # Bloqueia o acesso enquanto escrevemos no dicionário
    with _progress_lock:
        PROGRESS[task_id] = initial_state
        # Retornamos uma cópia para evitar que quem chamou modifique
        # o original sem passar pelo lock
        state_copy = copy.deepcopy(PROGRESS[task_id])

    # Persistência no DB (pode ficar fora do lock do dict para não travar leituras)
    try:
        new_task = TaskModel(
            id=task_id,
            phase="iniciando",
            message="Iniciando processo...",
            history=["Tarefa criada."]
        )
        db.session.add(new_task)
        db.session.commit()
    except Exception as e:
        print(f"Erro ao criar task no DB: {e}")
        db.session.rollback()

    return state_copy


def update_progress(task_id: str, updates: dict):
    """
    Função auxiliar nova para atualizar o progresso de forma segura.
    Use isso nos seus services ao invés de alterar PROGRESS[task_id] diretamente.
    """
    with _progress_lock:
        if task_id in PROGRESS:
            PROGRESS[task_id].update(updates)
            # Garante que o histórico não cresça infinitamente na memória
            if "history" in updates:
                # Se a atualização trouxe histórico, certifique-se de cortar o excesso
                # (Assumindo que o service já enviou a lista, mas por segurança:)
                current_hist = PROGRESS[task_id].get("history", [])
                if len(current_hist) > 100:
                    PROGRESS[task_id]["history"] = current_hist[-100:]


def sync_task_to_db(task_id: str):
    """
    Copia o estado atual da memória para o Banco de Dados.
    """
    # Leitura segura
    with _progress_lock:
        if task_id not in PROGRESS:
            return
        # Faz copy para liberar o lock rapidamente enquanto salvamos no banco
        data = copy.deepcopy(PROGRESS[task_id])

    try:
        task = TaskModel.query.get(task_id)
        if task:
            task.phase = data.get("phase")
            task.message = data.get("message")
            task.files_found = data.get("files_found", 0)
            task.files_total = data.get("files_total", 0)
            task.files_downloaded = data.get("files_downloaded", 0)
            task.bytes_found = data.get("bytes_found", 0)
            task.errors_count = data.get("errors", 0)
            task.canceled = data.get("canceled", False)
            task.history = list(data.get("history", []))

            db.session.commit()
    except Exception as e:
        print(f"Erro ao sincronizar task {task_id} pro DB: {e}")
        db.session.rollback()


def get_task_progress(task_id: str) -> dict:
    """
    Retorna o progresso de forma segura.
    """
    # 1. Memória (Prioridade)
    with _progress_lock:
        if task_id in PROGRESS:
            return copy.deepcopy(PROGRESS[task_id])

    # 2. Banco de Dados (Fallback)
    try:
        task = TaskModel.query.get(task_id)
        if task:
            return {
                "phase": task.phase,
                "message": task.message,
                "files_found": task.files_found,
                "files_total": task.files_total,
                "files_downloaded": task.files_downloaded,
                "bytes_found": task.bytes_found,
                "errors": task.errors_count,
                "history": task.history,
                "canceled": task.canceled,
                "from_db": True
            }
    except Exception:
        pass

    return {
        "phase": "desconhecido",
        "message": "Tarefa não encontrada.",
        "history": []
    }
