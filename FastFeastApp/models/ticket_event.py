"""TicketEvent — SQLAlchemy model for fact_ticket_event."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class TicketEvent(Base):

    event_id   = Column(String(36), primary_key=True)
    ticket_id  = Column(String(36), nullable=False)
    agent_id   = Column(Integer, nullable=False)
    event_ts   = Column(String(30), nullable=False)
    old_status = Column(String(10))
    new_status = Column(String(10), nullable=False)
    notes      = Column(String(36), nullable=False)
