import datetime
from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Customer(Base):
    customer_id:   int
    full_name:     str
    email:         str
    phone:         str
    region_id:     Decimal
    segment_id:    int
    signup_date:   datetime.date
    gender:        str
    created_at:    datetime.datetime = None
    updated_at:    datetime.datetime = None
    surrogate_key: str               = None
    is_current:    bool              = None
    valid_from:    datetime.date     = None
    valid_to:      datetime.date     = None