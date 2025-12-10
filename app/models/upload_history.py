# app/models/upload_history.py
from .db_instance import db
from app.utils.time_utils import get_sp_now

class UploadHistoryModel(db.Model):
    __tablename__ = "upload_history"

    id = db.Column(db.Integer, primary_key=True)
    
    # Metadados do Arquivo
    filename = db.Column(db.String(255), nullable=False)
    relative_path = db.Column(db.String(500), nullable=True) # Ex: "pasta/subpasta/arquivo.txt"
    mime_type = db.Column(db.String(100), nullable=True)
    size_bytes = db.Column(db.BigInteger, default=0)
    temp_path = db.Column(db.String(500), nullable=True)
    
    # Dados do Google Drive
    file_id = db.Column(db.String(100), nullable=True)       # ID gerado no Drive
    destination_id = db.Column(db.String(100), nullable=True)# ID da pasta pai
    
    # Controle
    status = db.Column(db.String(20), default="PENDING")     # PENDING, SUCCESS, ERROR
    error_message = db.Column(db.Text, nullable=True)
    
    
    # Auditoria
    user_email = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), default=get_sp_now)

    def to_dict(self):
        return {
            "id": self.id,
            "filename": self.filename,
            "relative_path": self.relative_path,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "file_id": self.file_id,
            "destination_id": self.destination_id,
            "status": self.status,
            "error_message": self.error_message,
            "user_email": self.user_email,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }