"""Restaurant — SQLAlchemy model for dim_restaurant (SCD Type 2)."""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, func
from models.base import Base


class Restaurant(Base):

    restaurant_id     = Column(Integer, primary_key=True, nullable=False)
    restaurant_name   = Column(String(19))
    region_id         = Column(Integer, nullable=False)
    category_id       = Column(Integer, nullable=False)
    price_tier        = Column(String(4), nullable=False)
    rating_avg        = Column(Numeric)
    prep_time_avg_min = Column(Integer, nullable=False)
    is_active         = Column(Boolean, nullable=False)
    created_at        = Column(DateTime, default=func.now())
    updated_at        = Column(DateTime, default=func.now(), onupdate=func.now())
    # SCD Type 2
    surrogate_key     = Column(String(36))
    is_current        = Column(Boolean)
    valid_from        = Column(Date)
    valid_to          = Column(Date)
