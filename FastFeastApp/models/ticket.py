"""Ticket — SQLAlchemy model for fact_ticket."""
import uuid
from sqlalchemy import Column, Integer, String, Numeric, DateTime
from models.base import Base


class Ticket(Base):

    ticket_id          = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id           = Column(String(36), nullable=False)
    customer_id        = Column(Integer, nullable=False)
    driver_id          = Column(Integer, nullable=False)
    restaurant_id      = Column(Integer, nullable=False)
    agent_id           = Column(Integer, nullable=False)
    reason_id          = Column(Integer, nullable=False)
    priority_id        = Column(Integer, nullable=False)
    channel_id         = Column(Integer, nullable=False)
    status             = Column(String(8), nullable=False)
    refund_amount      = Column(Numeric, nullable=False)
    created_at         = Column(DateTime, nullable=False)
    first_response_at  = Column(DateTime, nullable=False)
    resolved_at        = Column(DateTime, nullable=False)
    sla_first_due_at   = Column(DateTime, nullable=False)
    sla_resolve_due_at = Column(DateTime, nullable=False)
