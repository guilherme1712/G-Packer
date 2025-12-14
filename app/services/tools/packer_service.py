# app/services/packer_service.py
import os
import zipfile
import tarfile
import shutil
import time
from app.models import db, TaskModel

# Tenta importar py7zr
try:
    import py7zr
except ImportError:
    py7zr = None


class PackerService:

    @staticmethod
    def run_compression_task(app, task_id, input_dir, output_path, fmt, level):
        with app.app_context():
            task = TaskModel.query.get(task_id)
            if not task: return

            try:
                # 1. Início
                task.phase = "COMPRESSING"
                task.message = "Compactando arquivos..."
                task.files_downloaded = 0
                # Não setamos task.percent diretamente pois não é coluna
                db.session.commit()

                if level == "fast":
                    comp_level = 1
                elif level == "max":
                    comp_level = 9
                else:
                    comp_level = 5

                start_time = time.time()

                # 2. Compressão
                if fmt == "zip":
                    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=comp_level) as zf:
                        for root, dirs, filenames in os.walk(input_dir):
                            for fn in filenames:
                                abs_path = os.path.join(root, fn)
                                rel_path = os.path.relpath(abs_path, input_dir)
                                zf.write(abs_path, arcname=rel_path)

                elif "tar" in fmt:
                    mode = "w:gz" if "gz" in fmt else "w"
                    with tarfile.open(output_path, mode, compresslevel=comp_level) as tf:
                        tf.add(input_dir, arcname="")

                elif fmt == "7z":
                    if not py7zr: raise Exception("py7zr não instalado.")
                    filtros = [{'id': py7zr.FILTER_LZMA2, 'preset': comp_level}]
                    with py7zr.SevenZipFile(output_path, 'w', filters=filtros) as zf:
                        zf.writeall(input_dir, arcname="")

                # 3. Finalização
                duration = time.time() - start_time
                final_size = os.path.getsize(output_path)

                shutil.rmtree(input_dir, ignore_errors=True)

                task.phase = "COMPLETED"
                # Forçamos os contadores para o total para o model calcular 100%
                task.files_downloaded = task.files_total
                task.bytes_downloaded = final_size  # Armazena tamanho final
                task.message = f"Concluído ({round(final_size / 1024 / 1024, 2)} MB) em {round(duration, 1)}s"

                db.session.commit()

            except Exception as e:
                print(f"Erro Task {task_id}: {e}")
                task.phase = "ERROR"
                task.message = str(e)
                task.errors_count = (task.errors_count or 0) + 1
                db.session.commit()