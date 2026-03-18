"""ReasonCategory — SQLAlchemy model for dim_reason_category (SCD Type 0 — freeze)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class ReasonCategory(Base):

    reason_category_id = Column(Integer, primary_key=True)
    category_name      = Column(String(8), nullable=False)
