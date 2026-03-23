import datetime
from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class Agent(Base):
    agent_id:            int
    agent_name:          str
    agent_email:         str
    agent_phone:         int
    team_id:             int
    skill_level:         str
    hire_date:           datetime.date
    avg_handle_time_min: int
    resolution_rate:     Decimal
    csat_score:          Decimal
    is_active:           bool
    created_at:          datetime.datetime = None
    updated_at:          datetime.datetime = None