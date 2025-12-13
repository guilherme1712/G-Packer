# app/enum/task_type.py
from enum import Enum

class TaskTypeEnum(str, Enum):
    DOWNLOAD = "DOWNLOAD"
    UPLOAD = "UPLOAD"
    MAPPING = "MAPPING"  # <--- NOVO TIPO