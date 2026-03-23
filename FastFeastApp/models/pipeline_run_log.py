from decimal import Decimal
from dataclasses import dataclass
from models.base import Base

@dataclass
class PipelineRunLog(Base):
    run_id:                str
    file_name:             str
    total_records:         int
    null_rate:             Decimal
    orphan_rate:           Decimal
    processing_latency_ms: int
    scd_inserted_count:    int     = None
    scd_updated_count:     int     = None
    scd_expired_count:     int     = None
    file_success:          bool    = None