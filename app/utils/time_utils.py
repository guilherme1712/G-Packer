import pytz
from datetime import datetime

# Define o fuso horário padrão (São Paulo)
SP_TZ = pytz.timezone('America/Sao_Paulo')


def get_sp_now():
    """
    Retorna o datetime atual (aware) no fuso de São Paulo.
    Use esta função no 'default=' das colunas SQLAlchemy.
    """
    return datetime.now(SP_TZ)


def to_sp_timezone(dt_val):
    """
    Converte um datetime (naive ou aware) para o fuso de São Paulo.
    Útil para exibir datas que foram salvas como UTC ou naive no banco.
    """
    if dt_val is None:
        return None

    # Se não tem timezone (naive), assumimos que é UTC (padrão SQL)
    if dt_val.tzinfo is None:
        dt_val = pytz.utc.localize(dt_val)

    # Converte para SP
    return dt_val.astimezone(SP_TZ)


def format_sp_time(dt_val, fmt="%d/%m/%Y %H:%M"):
    """
    Converte para SP e formata como string.
    Use isso nos métodos .to_dict() dos modelos.
    """
    if not dt_val:
        return "-"

    sp_dt = to_sp_timezone(dt_val)
    return sp_dt.strftime(fmt)