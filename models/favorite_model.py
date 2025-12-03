from models import db

class FavoriteModel(db.Model):
    __tablename__ = 'favorites'
    
    # Se sua tabela users existir, você pode querer adicionar user_id aqui futuramente
    id = db.Column(db.String(255), primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    path = db.Column(db.Text, nullable=True)
    type = db.Column(db.String(50), default='folder')

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "path": self.path,
            "type": self.type
        }
