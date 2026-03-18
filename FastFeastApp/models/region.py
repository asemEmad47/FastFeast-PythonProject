"""Region — SQLAlchemy model for dim_region (SCD Type 1)."""
from sqlalchemy import Column, Integer, String, Numeric
from models.base import Base


class Region(Base):

    region_id         = Column(Integer, primary_key=True)
    region_name       = Column(String(12), nullable=False)
    city_id           = Column(Integer, nullable=False)
    delivery_base_fee = Column(Numeric, nullable=False)
