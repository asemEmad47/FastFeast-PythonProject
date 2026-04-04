"""
QuarantineWriter — DataFlowComponent.
Flushes accumulated bad_rows to quarantine storage.
Pass-through: does not modify data_frame_dict or metrics_dict.
Resets bad_rows to None after writing.
"""
from __future__ import annotations
from typing import Optional
import os, uuid
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry
# from config.settings import QUARANTINE_DIR


class QuarantineWriter(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None) -> None:
        super().__init__(audit=audit, registry=registry)

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        if bad_rows is not None and not bad_rows.empty:
            self._flush(bad_rows, reason=data_frame_dict.get("source", "unknown"))
            bad_rows = None   # reset after writing
        return True, [], data_frame_dict, metrics_dict, bad_rows

    # def _flush(self, df: pd.DataFrame, reason: str) -> None:
    #     """Write bad rows to quarantine directory as CSV."""
    #     try:
    #         os.makedirs(QUARANTINE_DIR, exist_ok=True)
    #         path = os.path.join(QUARANTINE_DIR, f"{reason}_{uuid.uuid4().hex}.csv")
    #         out  = df.copy()
    #         out["_quarantine_reason"] = reason
    #         out.to_csv(path, index=False)
    #         self.audit.track_metrics("quarantine", {
    #             "quarantined_count": len(out),
    #             "reason": reason,
    #         })
    #     except Exception as exc:
    #         self.audit.log_failure(f"QuarantineWriter failed: {exc}")
