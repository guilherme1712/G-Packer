import threading
import copy
import time
from models import db, TaskModel

# Cache em memória
PROGRESS: dict[str, dict] = {}

# LOCK para acesso seguro às threads
_progress_lock = threading.Lock()


def init_download_task(task_id: str) -> dict:
    """
    Inicializa a task na memória e cria o registro no Banco de Dados.
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
        "canceled": False,
        "paused": False,
    }

    with _progress_lock:
        PROGRESS[task_id] = initial_state
        state_copy = copy.deepcopy(PROGRESS[task_id])

    try:
        new_task = TaskModel(
            id=task_id,
            phase="iniciando",
            message="Iniciando processo...",
            history=["Tarefa criada."],
        )
        db.session.add(new_task)
        db.session.commit()
    except Exception as e:
        print(f"Erro ao criar task no DB: {e}")
        db.session.rollback()

    return state_copy


def update_progress(task_id: str, updates: dict):
    """
    Atualiza o progresso em memória.
    NOTA: Não removemos mais logs antigos (history) conforme solicitado.
    """
    with _progress_lock:
        if task_id in PROGRESS:
            # Se houver histórico na atualização, fazemos o append
            if "history" in updates:
                new_logs = updates.pop("history")
                current_hist = PROGRESS[task_id].get("history", [])
                # Concatena sem cortar
                if isinstance(new_logs, list):
                    current_hist.extend(new_logs)
                else:
                    current_hist.append(new_logs)
                PROGRESS[task_id]["history"] = current_hist

            PROGRESS[task_id].update(updates)


def sync_task_to_db(task_id: str):
    """
    Persiste o estado da memória para o Banco de Dados.
    """
    with _progress_lock:
        if task_id not in PROGRESS:
            return
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
            task.paused = data.get("paused", False)
            task.history = list(data.get("history", []))
            db.session.commit()
    except Exception as e:
        print(f"Erro ao sincronizar task {task_id}: {e}")
        db.session.rollback()


def get_task_progress(task_id: str) -> dict:
    """
    Lê o progresso (Memória -> DB).
    """
    with _progress_lock:
        if task_id in PROGRESS:
            return copy.deepcopy(PROGRESS[task_id])

    try:
        task = TaskModel.query.get(task_id)
        if task:
            return task.to_dict()
    except Exception:
        pass

    return {
        "phase": "desconhecido",
        "message": "Tarefa não encontrada.",
        "history": [],
    }


def get_all_active_tasks():
    """
    Retorna lista de tarefas que estão em memória (ativas recentemente).
    Usado para o widget flutuante.
    """
    with _progress_lock:
        # Filtra apenas o básico para a lista
        active = []
        for tid, data in PROGRESS.items():
            if data.get("phase") not in ["concluido", "erro", "cancelado"]:
                active.append({
                    "id": tid,
                    "phase": data.get("phase"),
                    "message": data.get("message"),
                    "paused": data.get("paused", False),
                    "canceled": data.get("canceled", False),
                    "percent": _calculate_percent(data)
                })
        return active

def _calculate_percent(data):
    phase = data.get("phase")
    if phase == 'mapeando': return 10
    if phase == 'compactando': return 90
    if phase == 'baixando':
        total = data.get("files_total", 0)
        done = data.get("files_downloaded", 0)
        if total > 0:
            return 20 + int((done/total) * 70)
        return 20
    return 0

# --- Controles de Estado ---

def set_task_pause(task_id: str, paused: bool):
    with _progress_lock:
        if task_id in PROGRESS:
            PROGRESS[task_id]["paused"] = paused
            msg = "PAUSADO pelo usuário" if paused else "RESUMIDO pelo usuário"
            PROGRESS[task_id]["history"].append(msg)
            PROGRESS[task_id]["message"] = msg
    sync_task_to_db(task_id)

def set_task_cancel(task_id: str):
    with _progress_lock:
        if task_id in PROGRESS:
            PROGRESS[task_id]["canceled"] = True
            PROGRESS[task_id]["history"].append("Solicitando cancelamento...")
    sync_task_to_db(task_id)