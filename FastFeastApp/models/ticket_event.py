from dataclasses import dataclass
from models.base import Base

@dataclass
class TicketEvent(Base):
    event_id:   str
    ticket_id:  str
    agent_id:   int
    event_ts:   str
    old_status: str
    new_status: str
    notes:      str