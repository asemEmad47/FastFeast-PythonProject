import datetime
from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Driver(Base):
    driver_id:            int
    driver_name:          str
    driver_phone:         str
    national_id:          int
    region_id:            int
    shift:                str
    vehicle_type:         str
    hire_date:            datetime.date
    rating_avg:           Decimal
    on_time_rate:         Decimal
    cancel_rate:          Decimal
    completed_deliveries: int
    is_active:            bool
    created_at:           datetime.datetime = None
    updated_at:           datetime.datetime = None
    surrogate_key:        str               = None
    is_current:           bool              = None
    valid_from:           datetime.date     = None
    valid_to:             datetime.date     = None
