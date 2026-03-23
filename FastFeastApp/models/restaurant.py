import datetime
from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Restaurant(Base):
    restaurant_id:     int
    restaurant_name:   str
    region_id:         int
    category_id:       int
    price_tier:        str
    rating_avg:        Decimal
    prep_time_avg_min: int
    is_active:         bool
    created_at:        datetime.datetime = None
    updated_at:        datetime.datetime = None
    surrogate_key:     str               = None
    is_current:        bool              = None
    valid_from:        datetime.date     = None
    valid_to:          datetime.date     = None