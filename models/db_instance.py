# models/db_instance.py
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sqlite3

# Instância global do SQLAlchemy
db = SQLAlchemy()


# Quando conectar no SQLite, aplica otimizações seguras
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()

        # Habilita modo WAL (permite leitura e escrita simultânea)
        cursor.execute("PRAGMA journal_mode=WAL;")

        # Reduz fsync, melhora velocidade sem comprometer segurança local
        cursor.execute("PRAGMA synchronous=NORMAL;")

        # Permite conexões paralelas
        cursor.execute("PRAGMA cache_size = -64000;")  # 64 MB de cache
        cursor.execute("PRAGMA temp_store = MEMORY;")

        cursor.close()
