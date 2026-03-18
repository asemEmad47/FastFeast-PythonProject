"""Reason — SQLAlchemy model for dim_reason (SCD Type 0 — freeze)."""
from sqlalchemy import Column, Integer, String, Numeric
from models.base import Base


class Reason(Base):

    reason_id          = Column(Integer, primary_key=True)
    reason_name        = Column(String(25), nullable=False)
    reason_category_id = Column(Integer, nullable=False)
    severity_level     = Column(Integer, nullable=False)
    typical_refund_pct = Column(Numeric, nullable=False)
