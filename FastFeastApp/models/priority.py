"""Priority — SQLAlchemy model for dim_priority (SCD Type 0 — freeze)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class Priority(Base):

    priority_id            = Column(Integer, primary_key=True)
    priority_code          = Column(String(2), nullable=False)
    priority_name          = Column(String(8), nullable=False)
    sla_first_response_min = Column(Integer, nullable=False)
    sla_resolution_min     = Column(Integer, nullable=False)
