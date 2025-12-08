import os
import time
from typing import Optional

try:
    from flask import current_app
except Exception:  # Flask pode não estar disponível em alguns contextos (tests/CLI)
    current_app = None  # type: ignore

import config as app_config


class StorageService:
    """
    Serviço centralizado para lidar com diretórios locais da aplicação.

    - Usa primeiro o current_app.config (quando existe contexto Flask)
    - Faz fallback para os valores do módulo config.py
    - Trata caminhos longos no Windows (prefixo \\?\\)
    - Garante criação de diretórios de forma robusta e thread-safe
    """

    # ---------------------------------------------------------
    # Helpers internos
    # ---------------------------------------------------------
    @classmethod
    def _get_cfg_path(cls, key: str, default: str) -> str:
        """
        Tenta ler um caminho do current_app.config; se não houver contexto
        de app ou a chave não existir, devolve o default.
        """
        if current_app is not None:
            try:
                value = current_app.config.get(key)  # type: ignore[attr-defined]
            except Exception:
                value = None
            if isinstance(value, str) and value:
                return value
        return default

    # ---------------------------------------------------------
    # Diretórios base
    # ---------------------------------------------------------
    @classmethod
    def base_dir(cls) -> str:
        return cls._get_cfg_path("BASE_DIR", app_config.BASE_DIR)

    @classmethod
    def storage_dir(cls, ensure: bool = True) -> str:
        default_storage = getattr(
            app_config,
            "STORAGE_DIR",
            os.path.join(app_config.BASE_DIR, "storage"),
        )
        path = cls._get_cfg_path("STORAGE_DIR", default_storage)
        if ensure:
            cls.ensure_dir(path)
        return path

    @classmethod
    def backups_dir(cls, ensure: bool = True) -> str:
        default_backups = getattr(
            app_config,
            "BACKUP_STORAGE_DIR",
            os.path.join(cls.storage_dir(False), "backups"),
        )
        path = cls._get_cfg_path("BACKUP_STORAGE_DIR", default_backups)
        if ensure:
            cls.ensure_dir(path)
        return path

    @classmethod
    def auth_dir(cls, ensure: bool = True) -> str:
        default_auth = getattr(
            app_config,
            "AUTH_STORAGE_DIR",
            os.path.join(cls.storage_dir(False), "auth"),
        )
        path = cls._get_cfg_path("AUTH_STORAGE_DIR", default_auth)
        if ensure:
            cls.ensure_dir(path)
        return path

    @classmethod
    def logs_dir(cls, ensure: bool = True) -> str:
        path = os.path.join(cls.storage_dir(False), "logs")
        if ensure:
            cls.ensure_dir(path)
        return path

    @classmethod
    def temp_work_dir(cls, ensure: bool = True) -> str:
        """
        Diretório base para trabalhos temporários de download/compactação.
        (equivalente antigo de "storage/temp_work")
        """
        path = os.path.join(cls.storage_dir(False), "temp_work")
        if ensure:
            cls.ensure_dir(path)
        return path

    # ---------------------------------------------------------
    # Utilitários de criação de diretórios
    # ---------------------------------------------------------
    @staticmethod
    def prepare_long_path(path: str) -> str:
        """
        Adiciona o prefixo \\?\\ em caminhos Windows para suportar
        mais de 260 caracteres.
        """
        if os.name == "nt":
            path = os.path.abspath(path)
            if not path.startswith("\\\\?\\"):
                return "\\\\?\\" + path
        return path

    @classmethod
    def ensure_dir(cls, path: str) -> str:
        """
        Cria o diretório informado, tratando condições de corrida
        e caminhos longos no Windows.
        """
        if not path:
            raise ValueError("Caminho de diretório vazio.")

        long_path = cls.prepare_long_path(path)

        try:
            os.makedirs(long_path, exist_ok=True)
        except OSError:
            # Em cenários de alta concorrência, mesmo exist_ok=True
            # pode falhar. Esperamos um pouco e rechecamos.
            time.sleep(0.05)
            if not os.path.isdir(long_path):
                head, tail = os.path.split(long_path)
                if head and not os.path.isdir(head):
                    cls.ensure_dir(head)
                if tail:
                    try:
                        os.mkdir(long_path)
                    except OSError:
                        # Se ainda assim falhar e o diretório realmente não existir,
                        # deixamos a exceção subir.
                        if not os.path.isdir(long_path):
                            raise
        return path

    @classmethod
    def ensure_parent_dir(cls, file_path: str) -> None:
        """
        Garante que a pasta pai de um arquivo exista.
        """
        directory = os.path.dirname(file_path)
        if directory:
            cls.ensure_dir(directory)

    @classmethod
    def backups_path_for(cls, filename: str, ensure: bool = True) -> str:
        """
        Retorna o caminho absoluto de um arquivo dentro da pasta de backups.
        """
        base = cls.backups_dir(ensure=ensure)
        return os.path.join(base, filename)
