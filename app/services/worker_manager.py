# app/services/worker_manager.py
import os
from concurrent.futures import ThreadPoolExecutor

class WorkerManager:
    """
    Gerenciador Central de Threads (Singleton).
    Isola os contextos de execução para evitar que um processo
    (ex: Download) consuma todos os recursos do sistema.
    """
    
    # --- CONFIGURAÇÃO DE LIMITES ---
    # Reduzi os valores para garantir estabilidade. 
    # 150 threads por contexto é muito agressivo para Python (GIL).
    # Ajuste conforme a capacidade do seu servidor.
    MAX_UPLOAD_WORKERS = 100
    MAX_DOWNLOAD_WORKERS = 100
    MAX_COMPRESSION_WORKERS = max(4, min(os.cpu_count() or 4, 8))
    
    _upload_executor = None
    _download_executor = None
    _compression_executor = None

    @classmethod
    def get_upload_executor(cls):
        """Pool dedicado para Uploads (QueueService)"""
        if cls._upload_executor is None:
            cls._upload_executor = ThreadPoolExecutor(
                max_workers=cls.MAX_UPLOAD_WORKERS, 
                thread_name_prefix="UploadWorker"
            )
        return cls._upload_executor

    @classmethod
    def get_download_executor(cls):
        """Pool dedicado para Downloads do Drive"""
        if cls._download_executor is None:
            cls._download_executor = ThreadPoolExecutor(
                max_workers=cls.MAX_DOWNLOAD_WORKERS, 
                thread_name_prefix="DownloadWorker"
            )
        return cls._download_executor

    @classmethod
    def get_compression_executor(cls):
        """Pool dedicado para Zip/Tar (CPU/IO Bound)"""
        if cls._compression_executor is None:
            cls._compression_executor = ThreadPoolExecutor(
                max_workers=cls.MAX_COMPRESSION_WORKERS, 
                thread_name_prefix="ZipWorker"
            )
        return cls._compression_executor

    @classmethod
    def shutdown_all(cls):
        """Limpeza graciosa ao desligar o app"""
        if cls._upload_executor: cls._upload_executor.shutdown(wait=False)
        if cls._download_executor: cls._download_executor.shutdown(wait=False)
        if cls._compression_executor: cls._compression_executor.shutdown(wait=False)