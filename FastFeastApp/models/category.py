from dataclasses import dataclass
from models.base import Base

@dataclass
class Category(Base):
    category_id:   int
    category_name: str