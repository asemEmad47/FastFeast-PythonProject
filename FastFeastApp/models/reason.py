from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Reason(Base):
    reason_id:          int
    reason_name:        str
    reason_category_id: int
    severity_level:     int
    typical_refund_pct: Decimal