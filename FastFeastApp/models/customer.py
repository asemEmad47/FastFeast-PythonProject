"""Customer — SQLAlchemy model for dim_customer (SCD Type 2)."""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, func
from models.base import Base


class Customer(Base):

    customer_id   = Column(Integer, primary_key=True, nullable=False)
    full_name     = Column(String(20))
    email         = Column(String(33))
    phone         = Column(String(11))
    region_id     = Column(Numeric)
    segment_id    = Column(Integer, nullable=False)
    signup_date   = Column(Date, nullable=False)
    gender        = Column(String(6), nullable=False)
    created_at    = Column(DateTime, default=func.now(), nullable=False)
    updated_at    = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    # SCD Type 2
    surrogate_key = Column(String(36))
    is_current    = Column(Boolean)
    valid_from    = Column(Date)
    valid_to      = Column(Date)
