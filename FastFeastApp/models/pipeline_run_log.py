"""PipelineRunLog — Audit table written by Audit.persist_to_dwh() after each run."""
from sqlalchemy import Column, Integer, String, Numeric, Boolean
from models.base import Base


class PipelineRunLog(Base):

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
