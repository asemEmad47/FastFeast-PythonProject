from dataclasses import dataclass
from models.base import Base

@dataclass
class Segment(Base):
    segment_id:       int
    segment_name:     str
    discount_pct:     int
    priority_support: bool