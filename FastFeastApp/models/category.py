"""Category — SQLAlchemy model for dim_category (SCD Type 0 — freeze)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class Category(Base):

    category_id   = Column(Integer, primary_key=True)
    category_name = Column(String(13), nullable=False)
