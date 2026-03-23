import datetime
from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Ticket(Base):
    ticket_id:          str
    order_id:           str
    customer_id:        int
    driver_id:          int
    restaurant_id:      int
    agent_id:           int
    reason_id:          int
    priority_id:        int
    channel_id:         int
    status:             str
    refund_amount:      Decimal
    created_at:         datetime.datetime
    first_response_at:  datetime.datetime
    resolved_at:        datetime.datetime
    sla_first_due_at:   datetime.datetime
    sla_resolve_due_at: datetime.datetime