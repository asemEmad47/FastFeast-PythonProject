"""Restaurant — SQLAlchemy model."""
from sqlalchemy import Column, Integer, String, Numeric, Boolean, DateTime, func
from repository.models.base_model import Base


class Restaurant(Base):
    __tablename__ = "dim_restaurant"

    restaurant_id   = Column(Integer, primary_key=True)
    restaurant_name = Column(String(19))
    region_id       = Column(Integer, nullable=False)
    category_id     = Column(Integer, nullable=False)
    price_tier      = Column(String(4), nullable=False)
    rating_avg      = Column(Numeric)
    prep_time_avg_min = Column(Integer, nullable=False)
    is_active       = Column(Boolean, nullable=False)
    created_at      = Column(DateTime, default=func.now())
    updated_at      = Column(DateTime, default=func.now(), onupdate=func.now())
    surrogate_key   = Column(String(36))
    is_current      = Column(Boolean)
    valid_from      = Column(String(10))
    valid_to        = Column(String(10))
