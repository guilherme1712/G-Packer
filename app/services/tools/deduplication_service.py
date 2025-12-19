# app/services/tools/deduplication_service.py
from sqlalchemy import func, desc
from app.models import db, DriveItemCacheModel
from app.services.google.drive_ops import DriveOperationsService
# Precisamos importar o construtor do serviço para chamar a API
from app.services.google.drive_tree import get_thread_safe_service 

class DeduplicationService:

    @staticmethod
    def get_drive_quota(creds):
        """
        Busca informações de armazenamento diretamente da API do Google.
        Retorna dict com 'limit', 'usage', 'usageInDrive', 'usageInTrash'.
        """
        try:
            service = get_thread_safe_service(creds)
            about = service.about().get(fields="storageQuota").execute()
            return about.get('storageQuota', {})
        except Exception as e:
            print(f"Erro ao buscar quota: {e}")
            return {}

    @staticmethod
    def get_storage_analysis():
        """
        Retorna estatísticas para os gráficos do Dashboard (Baseado no Cache Local).
        """
        results = db.session.query(
            DriveItemCacheModel.mime_type,
            func.sum(DriveItemCacheModel.size_bytes).label('total_size'),
            func.count(DriveItemCacheModel.id).label('count')
        ).filter(
            DriveItemCacheModel.trashed == False,
            DriveItemCacheModel.is_folder == False
        ).group_by(DriveItemCacheModel.mime_type).all()

        data = []
        for r in results:
            data.append({
                "mime_type": r.mime_type,
                "total_size": int(r.total_size or 0),
                "count": r.count
            })
        
        data.sort(key=lambda x: x['total_size'], reverse=True)
        return data

    @staticmethod
    def find_duplicates():
        # ... (código existente mantido) ...
        subquery = db.session.query(DriveItemCacheModel.md5_checksum)\
            .filter(
                DriveItemCacheModel.trashed == False,
                DriveItemCacheModel.is_folder == False,
                DriveItemCacheModel.md5_checksum != None
            )\
            .group_by(DriveItemCacheModel.md5_checksum)\
            .having(func.count(DriveItemCacheModel.id) > 1)\
            .subquery()

        duplicates = DriveItemCacheModel.query.filter(
            DriveItemCacheModel.md5_checksum.in_(subquery)
        ).order_by(
            DriveItemCacheModel.md5_checksum, 
            desc(DriveItemCacheModel.modified_time)
        ).all()

        grouped = {}
        for item in duplicates:
            if item.md5_checksum not in grouped:
                grouped[item.md5_checksum] = {
                    "md5": item.md5_checksum,
                    "size_bytes": item.size_bytes,
                    "files": []
                }
            
            grouped[item.md5_checksum]["files"].append({
                "id": item.drive_id,
                "name": item.name,
                "path": item.path,
                "modified_time": item.modified_time,
                "parent_id": item.parent_id
            })

        return list(grouped.values())

    @staticmethod
    def find_large_files(limit=50, min_size_mb=10):
        # ... (código existente mantido) ...
        min_bytes = min_size_mb * 1024 * 1024
        
        files = DriveItemCacheModel.query.filter(
            DriveItemCacheModel.trashed == False,
            DriveItemCacheModel.is_folder == False,
            DriveItemCacheModel.size_bytes > min_bytes
        ).order_by(desc(DriveItemCacheModel.size_bytes)).limit(limit).all()

        return [f.to_dict() for f in files]

    @staticmethod
    def bulk_trash_files(creds, file_ids_list):
        # ... (código existente mantido) ...
        results = {
            "success": [],
            "failed": []
        }
        
        for fid in file_ids_list:
            res = DriveOperationsService.trash_file(creds, fid)
            if res.get("ok"):
                results["success"].append(fid)
            else:
                results["failed"].append({"id": fid, "error": res.get("error")})
                
        return results