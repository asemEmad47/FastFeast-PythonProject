from dataclasses import dataclass
from models.base import Base

@dataclass
class ReasonCategory(Base):
    reason_category_id: int
    category_name:      str
