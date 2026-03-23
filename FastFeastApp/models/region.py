from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Region(Base):
    region_id:         int
    region_name:       str
    city_id:           int
    delivery_base_fee: Decimal