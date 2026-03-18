"""Driver — SQLAlchemy model for dim_driver (SCD Type 2)."""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, func
from models.base import Base


class Driver(Base):

    driver_id            = Column(Integer, primary_key=True, nullable=False)
    driver_name          = Column(String(19))
    driver_phone         = Column(String(11))
    national_id          = Column(Integer, nullable=False)
    region_id            = Column(Integer, nullable=False)
    shift                = Column(String(7), nullable=False)
    vehicle_type         = Column(String(9), nullable=False)
    hire_date            = Column(Date, nullable=False)
    rating_avg           = Column(Numeric, nullable=False)
    on_time_rate         = Column(Numeric, nullable=False)
    cancel_rate          = Column(Numeric, nullable=False)
    completed_deliveries = Column(Integer, nullable=False)
    is_active            = Column(Boolean, nullable=False)
    created_at           = Column(DateTime, default=func.now())
    updated_at           = Column(DateTime, default=func.now(), onupdate=func.now())
    # SCD Type 2
    surrogate_key        = Column(String(36))
    is_current           = Column(Boolean)
    valid_from           = Column(Date)
    valid_to             = Column(Date)
