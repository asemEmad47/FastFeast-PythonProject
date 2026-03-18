"""City — SQLAlchemy model for dim_city (SCD Type 0 — freeze)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class City(Base):

    city_id   = Column(Integer, primary_key=True)
    city_name = Column(String(10), nullable=False)
    country   = Column(String(5), nullable=False)
    timezone  = Column(String(12), nullable=False)
