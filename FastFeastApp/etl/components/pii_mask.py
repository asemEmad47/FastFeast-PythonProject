"""
PIIMask — DataFlowComponent.
Hashes PII fields (email, phone, national_id, etc.) before data reaches any storage.
Field list comes from pipeline.yaml → pii_fields.
"""
from typing import Optional
import hashlib
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit


class PIIMask(DataFlowComponent):

    def __init__(self, pii_fields: list[str], audit: Audit) -> None:
        super().__init__(audit=audit)
        self.pii_fields = pii_fields

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["PIIMask received None df"], None
        try:
            masked = df.copy()
            for field in self.pii_fields:
                if field in masked.columns:
                    masked[field] = masked[field].apply(
                        lambda v: hashlib.sha256(str(v).encode()).hexdigest() if pd.notna(v) else v
                    )
            return True, [], masked
        except Exception as exc:
            return False, [f"PIIMask failed: {exc}"], df
