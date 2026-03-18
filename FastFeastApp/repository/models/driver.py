"""Driver — SQLAlchemy model for dim_driver."""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, func
from repository.models.base_model import Base


class Driver(Base):
    __tablename__ = "dim_driver"

    driver_id             = Column(Integer, primary_key=True)
    driver_name           = Column(String(19))
    driver_phone          = Column(String(11))
    national_id           = Column(Integer, nullable=False)
    region_id             = Column(Integer, nullable=False)
    shift                 = Column(String(7), nullable=False)
    vehicle_type          = Column(String(9), nullable=False)
    hire_date             = Column(Date, nullable=False)
    rating_avg            = Column(Numeric, nullable=False)
    on_time_rate          = Column(Numeric, nullable=False)
    cancel_rate           = Column(Numeric, nullable=False)
    completed_deliveries  = Column(Integer, nullable=False)
    is_active             = Column(Boolean, nullable=False)
    created_at            = Column(DateTime, default=func.now())
    updated_at            = Column(DateTime, default=func.now(), onupdate=func.now())
    surrogate_key         = Column(String(36))
    is_current            = Column(Boolean)
    valid_from            = Column(Date)
    valid_to              = Column(Date)
