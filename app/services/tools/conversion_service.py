import os
import shutil
import json
import yaml
import xmltodict
import markdown
import fitz  # PyMuPDF
import pandas as pd
from PIL import Image, ImageOps
from pdf2docx import Converter
from moviepy.editor import VideoFileClip, AudioFileClip
from app.services.storage import StorageService
from app.services.extraction_service import ExtractionService  # Certifique-se de ter este serviço


class ConversionService:

    @staticmethod
    def convert_file(input_path, target_format, output_folder, params=None):
        """
        Hub universal de conversão.
        """
        if params is None: params = {}

        filename = os.path.basename(input_path)
        name, ext = os.path.splitext(filename)
        ext = ext.lower().replace('.', '')
        target_format = target_format.lower()

        output_file = os.path.join(output_folder, f"{name}.{target_format}")

        try:
            # ==========================================
            # 1. CATEGORIA: PDF & DOCUMENTOS
            # ==========================================
            if ext == 'pdf':
                if target_format == 'docx':
                    cv = Converter(input_path)
                    cv.convert(output_file, start=0, end=None)
                    cv.close()
                    return True, output_file, "PDF convertido para Word."

                elif target_format == 'txt':
                    doc = fitz.open(input_path)
                    text = "".join([page.get_text() for page in doc])
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(text)
                    return True, output_file, "Texto extraído do PDF."

                elif target_format in ['jpg', 'png', 'webp']:
                    return ConversionService._pdf_to_images(input_path, output_folder, target_format)

            # ==========================================
            # 2. CATEGORIA: IMAGENS
            # ==========================================
            IMAGE_EXTS = ['jpg', 'jpeg', 'png', 'webp', 'bmp', 'tiff', 'ico', 'gif']
            if ext in IMAGE_EXTS:
                if target_format == 'pdf':
                    img = Image.open(input_path)
                    if img.mode != 'RGB': img = img.convert('RGB')
                    img.save(output_file, "PDF", resolution=100.0)
                    return True, output_file, "Imagem salva como PDF."

                elif target_format in IMAGE_EXTS:
                    img = Image.open(input_path)

                    if target_format in ['jpg', 'jpeg', 'bmp'] and img.mode in ('RGBA', 'P'):
                        img = img.convert('RGB')

                    if params.get('grayscale'):
                        img = ImageOps.grayscale(img)

                    if params.get('resize_pct'):
                        scale = int(params['resize_pct']) / 100.0
                        new_size = (int(img.width * scale), int(img.height * scale))
                        img = img.resize(new_size, Image.Resampling.LANCZOS)

                    img.save(output_file, quality=90)
                    return True, output_file, f"Imagem convertida para {target_format.upper()}."

            # ==========================================
            # 3. CATEGORIA: DADOS (TABELAS)
            # ==========================================
            DATA_EXTS = ['csv', 'xlsx', 'xls', 'json', 'xml']
            if ext in DATA_EXTS:
                df = None
                # Leitura
                if ext == 'csv':
                    df = pd.read_csv(input_path)
                elif ext in ['xlsx', 'xls']:
                    df = pd.read_excel(input_path)
                elif ext == 'json':
                    df = pd.read_json(input_path)
                elif ext == 'xml':
                    df = pd.read_xml(input_path)

                # Escrita
                if df is not None:
                    if target_format == 'csv':
                        df.to_csv(output_file, index=False)
                    elif target_format == 'xlsx':
                        df.to_excel(output_file, index=False)
                    elif target_format == 'json':
                        df.to_json(output_file, orient='records', indent=4, force_ascii=False)
                    elif target_format == 'html':
                        df.to_html(output_file, index=False, classes='table table-striped')
                    elif target_format == 'markdown':  # .md
                        with open(output_file, 'w', encoding='utf-8') as f:
                            f.write(df.to_markdown(index=False))

                    return True, output_file, f"Dados convertidos de {ext.upper()} para {target_format.upper()}."

            # ==========================================
            # 4. CATEGORIA: ARQUIVOS COMPACTADOS
            # ==========================================
            ARCHIVE_EXTS = ['zip', '7z', 'rar', 'tar', 'gz', 'tgz']
            if ext in ARCHIVE_EXTS:
                temp_extract = os.path.join(output_folder, "temp_extract")
                success, msg = ExtractionService.identify_and_extract(input_path, temp_extract)

                if not success: return False, None, msg

                base_name = os.path.join(output_folder, f"{name}_convertido")

                if target_format == '7z':
                    import py7zr
                    final_path = f"{base_name}.7z"
                    with py7zr.SevenZipFile(final_path, 'w') as z:
                        z.writeall(temp_extract, arcname='')
                else:
                    format_shutil = 'gztar' if target_format == 'tar.gz' else target_format
                    shutil.make_archive(base_name, format_shutil, temp_extract)
                    final_path = f"{base_name}.{target_format}"
                    if target_format == 'gztar': final_path = f"{base_name}.tar.gz"

                shutil.rmtree(temp_extract, ignore_errors=True)
                return True, final_path, f"Arquivo convertido para {target_format.upper()}."

            # ==========================================
            # 5. CATEGORIA: MULTIMÍDIA (VÍDEO & ÁUDIO)
            # ==========================================
            MEDIA_EXTS = ['mp4', 'avi', 'mov', 'mkv', 'mp3', 'wav', 'ogg']

            if ext in MEDIA_EXTS:
                if ext in ['mp4', 'avi', 'mov', 'mkv'] and target_format == 'mp3':
                    video = VideoFileClip(input_path)
                    video.audio.write_audiofile(output_file, logger=None)
                    video.close()
                    return True, output_file, "Áudio extraído do vídeo."

                elif ext in ['avi', 'mov', 'mkv'] and target_format == 'mp4':
                    clip = VideoFileClip(input_path)
                    clip.write_videofile(output_file, codec='libx264', audio_codec='aac', logger=None)
                    clip.close()
                    return True, output_file, "Vídeo convertido para MP4."

                elif ext in ['mp4', 'avi'] and target_format == 'gif':
                    clip = VideoFileClip(input_path)
                    duration = min(clip.duration, 10)
                    subclip = clip.subclip(0, duration).resize(width=480)
                    subclip.write_gif(output_file, logger=None)
                    clip.close()
                    return True, output_file, f"Vídeo convertido para GIF ({duration}s)."

                elif ext == 'wav' and target_format == 'mp3':
                    audio = AudioFileClip(input_path)
                    audio.write_audiofile(output_file, logger=None)
                    audio.close()
                    return True, output_file, "Áudio convertido para MP3."

            # ==========================================
            # 6. CATEGORIA: DEV & CONFIG
            # ==========================================
            DEV_EXTS = ['json', 'yaml', 'yml', 'xml']
            if ext in DEV_EXTS:
                data = None
                with open(input_path, 'r', encoding='utf-8') as f:
                    if ext == 'json':
                        data = json.load(f)
                    elif ext in ['yaml', 'yml']:
                        data = yaml.safe_load(f)
                    elif ext == 'xml':
                        data = xmltodict.parse(f.read())

                if data:
                    with open(output_file, 'w', encoding='utf-8') as f:
                        if target_format == 'json':
                            json.dump(data, f, indent=4, ensure_ascii=False)
                        elif target_format == 'yaml':
                            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
                        elif target_format == 'xml':
                            if len(data.keys()) > 1: data = {'root': data}
                            xmltodict.unparse(data, output=f, pretty=True)
                    return True, output_file, f"Convertido para {target_format.upper()}."

            # ==========================================
            # 7. CATEGORIA: WEB
            # ==========================================
            if ext == 'md' and target_format == 'html':
                with open(input_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                html = markdown.markdown(text)
                full_html = f"<html><body style='font-family:sans-serif;padding:2rem;'>{html}</body></html>"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(full_html)
                return True, output_file, "Markdown convertido para HTML."

            return False, None, "Formato não suportado."

        except Exception as e:
            return False, None, f"Erro interno: {str(e)}"

    @staticmethod
    def _pdf_to_images(pdf_path, output_folder, img_format):
        doc = fitz.open(pdf_path)
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        img_dir = os.path.join(output_folder, base)
        os.makedirs(img_dir, exist_ok=True)

        for i, page in enumerate(doc):
            pix = page.get_pixmap(dpi=150)
            pix.save(os.path.join(img_dir, f"pag_{i + 1:03d}.{img_format}"))

        zip_path = os.path.join(output_folder, f"{base}_imagens")
        shutil.make_archive(zip_path, 'zip', img_dir)
        shutil.rmtree(img_dir)
        return True, f"{zip_path}.zip", "PDF convertido para imagens (ZIP)."