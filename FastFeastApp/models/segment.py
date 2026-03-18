"""Segment — SQLAlchemy model for dim_segment (SCD Type 1)."""
from sqlalchemy import Column, Integer, String, Boolean
from models.base import Base


class Segment(Base):

    segment_id       = Column(Integer, primary_key=True)
    segment_name     = Column(String(7), nullable=False)
    discount_pct     = Column(Integer, nullable=False)
    priority_support = Column(Boolean, nullable=False)
