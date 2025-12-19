import os
import shutil
import time
from flask import Blueprint, render_template, request, flash, redirect, send_file
from app.services.tools.conversion_service import ConversionService
from app.services.storage import StorageService

tools_bp = Blueprint('tools', __name__, url_prefix='/tools')


@tools_bp.route('/converter', methods=['GET', 'POST'])
def converter():
    if request.method == 'POST':
        if 'file_convert' not in request.files:
            flash('Selecione um arquivo.', 'danger')
            return redirect(request.url)

        file = request.files['file_convert']
        target_format = request.form.get('target_format')

        params = {
            'resize_pct': request.form.get('resize_pct'),
            'grayscale': 'grayscale' in request.form
        }

        if not file or not target_format:
            flash('Configuração inválida.', 'warning')
            return redirect(request.url)

        temp_dir = StorageService.temp_work_dir()
        job_id = f"conv_{int(time.time())}"
        job_dir = os.path.join(temp_dir, job_id)
        StorageService.ensure_dir(job_dir)

        input_path = os.path.join(job_dir, file.filename)

        try:
            file.save(input_path)

            success, result_path, msg = ConversionService.convert_file(
                input_path,
                target_format,
                job_dir,
                params
            )

            if success and result_path and os.path.exists(result_path):
                return send_file(
                    result_path,
                    as_attachment=True,
                    download_name=os.path.basename(result_path)
                )
            else:
                shutil.rmtree(job_dir, ignore_errors=True)
                flash(f"Falha: {msg}", 'danger')
                return redirect(request.url)

        except Exception as e:
            shutil.rmtree(job_dir, ignore_errors=True)
            flash(f"Erro: {e}", 'danger')
            return redirect(request.url)

    return render_template('tools/converter.html')