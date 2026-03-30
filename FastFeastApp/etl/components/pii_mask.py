"""
PIIMask — DataFlowComponent.
Hashes PII fields before data reaches any storage.
Pass-through for metrics and bad_rows.
"""
from __future__ import annotations
from typing import Optional
import hashlib
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class PIIMask(DataFlowComponent):

    def __init__(
        self,
        pii_fields: list[str],
        audit:      Audit,
        registry:   DataRegistry = None,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.pii_fields = pii_fields

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = self.get_df(data_frame_dict)
        if df is None:
            return False, ["PIIMask received None df"], data_frame_dict, metrics_dict, bad_rows
        try:
            masked = df.copy()
            for field in self.pii_fields:
                if field in masked.columns:
                    masked[field] = masked[field].apply(
                        lambda v: hashlib.sha256(str(v).encode()).hexdigest()
                        if pd.notna(v) else v
                    )
            self.set_df(data_frame_dict, masked)
            return True, [], data_frame_dict, metrics_dict, bad_rows
        except Exception as exc:
            return False, [f"PIIMask failed: {exc}"], data_frame_dict, metrics_dict, bad_rows
