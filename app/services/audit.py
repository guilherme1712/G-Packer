# app/services/audit.py
import json
from flask import request
from app.models import db, AuditLogModel, GoogleAuthModel

class AuditService:
    @staticmethod
    def get_current_user_email():
        """Retorna o email da conta Google ativa no momento."""
        try:
            auth = GoogleAuthModel.query.filter_by(active=True).first()
            return auth.email if auth else "sistema@local"
        except:
            return "desconhecido"

    @staticmethod
    def log(action_type, target, ip_address=None, details=None, user_email=None):
        """
        Registra uma ação no log.
        Se ip_address não for passado, tenta pegar do request atual.
        Se user_email não for passado, pega do banco.
        """
        try:
            if not user_email:
                user_email = AuditService.get_current_user_email()

            if not ip_address:
                # Tenta pegar do contexto do Flask (se estiver numa rota)
                try:
                    ip_address = request.remote_addr
                except:
                    ip_address = "127.0.0.1"

            # Converte dict/list para string JSON se necessário
            if isinstance(details, (dict, list)):
                details = json.dumps(details, ensure_ascii=False)

            new_log = AuditLogModel(
                user_email=user_email,
                action_type=action_type,
                target=str(target),
                ip_address=ip_address,
                details=str(details) if details else ""
            )

            db.session.add(new_log)
            db.session.commit()
        except Exception as e:
            print(f"[AUDIT ERROR] Falha ao registrar log: {e}")
            db.session.rollback()

    @staticmethod
    def fetch_logs(start_date=None, end_date=None, action=None, limit=200):
        query = AuditLogModel.query.order_by(AuditLogModel.created_at.desc())

        if start_date:
            query = query.filter(AuditLogModel.created_at >= start_date)
        if end_date:
            query = query.filter(AuditLogModel.created_at <= end_date)
        if action and action != "todos":
            query = query.filter(AuditLogModel.action_type == action)

        return query.limit(limit).all()
