"""Agent — SQLAlchemy model for dim_agent."""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, func
from repository.models.base_model import Base


class Agent(Base):
    __tablename__ = "dim_agent"

    agent_id            = Column(Integer, primary_key=True)
    agent_name          = Column(String(19), nullable=False)
    agent_email         = Column(String(32), nullable=False)
    agent_phone         = Column(Integer, nullable=False)
    team_id             = Column(Integer, nullable=False)
    skill_level         = Column(String(6), nullable=False)
    hire_date           = Column(Date, nullable=False)
    avg_handle_time_min = Column(Integer, nullable=False)
    resolution_rate     = Column(Numeric, nullable=False)
    csat_score          = Column(Numeric, nullable=False)
    is_active           = Column(Boolean, nullable=False)
    created_at          = Column(DateTime, default=func.now())
    updated_at          = Column(DateTime, default=func.now(), onupdate=func.now())
