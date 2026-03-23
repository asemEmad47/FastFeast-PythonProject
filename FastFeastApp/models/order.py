from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Order(Base):
    order_id:         str
    customer_id:      int
    restaurant_id:    int
    driver_id:        int
    region_id:        int
    order_amount:     Decimal
    delivery_fee:     Decimal
    discount_amount:  int
    total_amount:     Decimal
    order_status:     str
    payment_method:   str
    order_created_at: str
    delivered_at:     str = None