from dataclasses import dataclass
from models.base import Base

@dataclass
class Priority(Base):
    priority_id:            int
    priority_code:          str
    priority_name:          str
    sla_first_response_min: int
    sla_resolution_min:     int
