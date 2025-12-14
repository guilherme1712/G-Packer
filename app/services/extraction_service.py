import os
import zipfile
import tarfile
import py7zr
from werkzeug.datastructures import FileStorage
from app.services.storage import StorageService


class ExtractionService:

    @staticmethod
    def identify_and_extract(file_path: str, extract_to: str):
        """
        Identifica o tipo de arquivo pela extensão e extrai para o destino.
        Retorna: (sucesso: bool, mensagem: str)
        """
        StorageService.ensure_dir(extract_to)

        filename = file_path.lower()

        try:
            # Lógica para ZIP
            if filename.endswith('.zip'):
                if not zipfile.is_zipfile(file_path):
                    return False, "Arquivo ZIP inválido ou corrompido."
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                return True, "Extração ZIP concluída."

            # Lógica para 7-Zip (.7z)
            elif filename.endswith('.7z'):
                if not py7zr.is_7zfile(file_path):
                    return False, "Arquivo 7z inválido ou corrompido."
                with py7zr.SevenZipFile(file_path, mode='r') as z:
                    z.extractall(path=extract_to)
                return True, "Extração 7-Zip concluída."

            # Lógica para TAR (tar.gz, .tgz, .tar)
            elif filename.endswith(('.tar.gz', '.tgz', '.tar', '.gz')):
                if not tarfile.is_tarfile(file_path):
                    return False, "Arquivo TAR inválido."
                mode = "r:gz" if filename.endswith("gz") else "r:"
                with tarfile.open(file_path, mode) as tar_ref:
                    def is_within_directory(directory, target):
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        return prefix == abs_directory

                    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                        for member in tar.getmembers():
                            member_path = os.path.join(path, member.name)
                            if not is_within_directory(path, member_path):
                                raise Exception("Tentativa de Path Traversal no arquivo TAR")
                        tar.extractall(path, members, numeric_owner=numeric_owner)

                    safe_extract(tar_ref, extract_to)
                return True, "Extração TAR concluída."

            else:
                return False, "Formato de arquivo não suportado para extração."

        except Exception as e:
            return False, f"Erro na extração: {str(e)}"

    @staticmethod
    def handle_upload_and_extract(uploaded_file: FileStorage, destination_folder: str = None):
        """
        Recebe o arquivo do form, salva temporariamente e extrai.
        Se destination_folder for None, cria uma pasta 'extracted_files' na raiz.
        """
        if not uploaded_file or uploaded_file.filename == '':
            return False, "Nenhum arquivo enviado."

        # 1. Preparar caminhos
        temp_dir = StorageService.temp_work_dir()
        filename = uploaded_file.filename
        save_path = os.path.join(temp_dir, filename)

        # Define onde vai extrair (cria uma pasta com o nome do arquivo)
        folder_name = os.path.splitext(filename)[0]

        if not destination_folder:
            base_extract = os.path.join(os.getcwd(), 'extracted_files')
        else:
            base_extract = destination_folder

        final_dest = os.path.join(base_extract, folder_name)

        try:
            # 2. Salvar o arquivo compactado temporariamente
            uploaded_file.save(save_path)

            # 3. Executar extração
            success, message = ExtractionService.identify_and_extract(save_path, final_dest)

            # 4. Limpar o arquivo compactado temporariamente
            if os.path.exists(save_path):
                os.remove(save_path)

            if success:
                return True, f"Arquivos extraídos com sucesso."
            else:
                return False, message

        except Exception as e:
            return False, f"Erro crítico: {str(e)}"