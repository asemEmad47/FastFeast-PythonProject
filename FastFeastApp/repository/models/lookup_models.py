"""
Small lookup / reference table models.
All SCD Type 0 or Type 1 — no history columns needed.
"""
from sqlalchemy import Column, Integer, String, Numeric, Boolean
from repository.models.base_model import Base


class Priority(Base):
    __tablename__ = "dim_priority"
    priority_id           = Column(Integer, primary_key=True)
    priority_code         = Column(String(2), nullable=False)
    priority_name         = Column(String(8), nullable=False)
    sla_first_response_min= Column(Integer, nullable=False)
    sla_resolution_min    = Column(Integer, nullable=False)


class Channel(Base):
    __tablename__ = "dim_channel"
    channel_id   = Column(Integer, primary_key=True)
    channel_name = Column(String(5), nullable=False)


class Region(Base):
    __tablename__ = "dim_region"
    region_id          = Column(Integer, primary_key=True)
    region_name        = Column(String(12), nullable=False)
    city_id            = Column(Integer, nullable=False)
    delivery_base_fee  = Column(Numeric, nullable=False)


class City(Base):
    __tablename__ = "dim_city"
    city_id   = Column(Integer, primary_key=True)
    city_name = Column(String(10), nullable=False)
    country   = Column(String(5), nullable=False)
    timezone  = Column(String(12), nullable=False)


class Segment(Base):
    __tablename__ = "dim_segment"
    segment_id      = Column(Integer, primary_key=True)
    segment_name    = Column(String(7), nullable=False)
    discount_pct    = Column(Integer, nullable=False)
    priority_support= Column(Boolean, nullable=False)


class Team(Base):
    __tablename__ = "dim_team"
    team_id   = Column(Integer, primary_key=True)
    team_name = Column(String(15), nullable=False)


class Category(Base):
    __tablename__ = "dim_category"
    category_id   = Column(Integer, primary_key=True)
    category_name = Column(String(13), nullable=False)


class Reason(Base):
    __tablename__ = "dim_reason"
    reason_id           = Column(Integer, primary_key=True)
    reason_name         = Column(String(25), nullable=False)
    reason_category_id  = Column(Integer, nullable=False)
    severity_level      = Column(Integer, nullable=False)
    typical_refund_pct  = Column(Numeric, nullable=False)


class ReasonCategory(Base):
    __tablename__ = "dim_reason_category"
    reason_category_id = Column(Integer, primary_key=True)
    category_name      = Column(String(8), nullable=False)


class Order(Base):
    __tablename__ = "fact_order"
    order_id       = Column(String(36), primary_key=True)
    customer_id    = Column(Integer, nullable=False)
    restaurant_id  = Column(Integer, nullable=False)
    driver_id      = Column(Integer, nullable=False)
    region_id      = Column(Integer, nullable=False)
    order_amount   = Column(Numeric, nullable=False)
    delivery_fee   = Column(Numeric, nullable=False)
    discount_amount= Column(Integer, nullable=False)
    total_amount   = Column(Numeric, nullable=False)
    order_status   = Column(String(9), nullable=False)
    payment_method = Column(String(6), nullable=False)
    order_created_at= Column(String(30), nullable=False)
    delivered_at   = Column(String(30))


class TicketEvent(Base):
    __tablename__ = "fact_ticket_event"
    event_id   = Column(String(36), primary_key=True)
    ticket_id  = Column(String(36), nullable=False)
    agent_id   = Column(Integer, nullable=False)
    event_ts   = Column(String(30), nullable=False)
    old_status = Column(String(10))
    new_status = Column(String(10), nullable=False)
    notes      = Column(String(36), nullable=False)


class PipelineRunLog(Base):
    __tablename__ = "pipeline_run_log"
    run_id                = Column(String(36), primary_key=True)
    file_name             = Column(String(255), nullable=False)
    total_records         = Column(Integer, nullable=False)
    null_rate             = Column(Numeric, nullable=False)
    orphan_rate           = Column(Numeric, nullable=False)
    processing_latency_ms = Column(Integer, nullable=False)
    scd_inserted_count    = Column(Integer)
    scd_updated_count     = Column(Integer)
    scd_expired_count     = Column(Integer)
    file_success          = Column(Boolean, nullable=False)
