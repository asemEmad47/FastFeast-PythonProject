"""Order — SQLAlchemy model for fact_order."""
from sqlalchemy import Column, Integer, String, Numeric
from models.base import Base


class Order(Base):

    order_id         = Column(String(36), primary_key=True)
    customer_id      = Column(Integer, nullable=False)
    restaurant_id    = Column(Integer, nullable=False)
    driver_id        = Column(Integer, nullable=False)
    region_id        = Column(Integer, nullable=False)
    order_amount     = Column(Numeric, nullable=False)
    delivery_fee     = Column(Numeric, nullable=False)
    discount_amount  = Column(Integer, nullable=False)
    total_amount     = Column(Numeric, nullable=False)
    order_status     = Column(String(9), nullable=False)
    payment_method   = Column(String(6), nullable=False)
    order_created_at = Column(String(30), nullable=False)
    delivered_at     = Column(String(30))
