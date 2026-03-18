"""
QuarantineWriter — DataFlowComponent.
Writes rejected rows to the quarantine path from config.
Called internally by ValidatorComponent and OrphansHandler.
As a chain component it is a pass-through (returns the same df it received).
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from config.settings import QUARANTINE_DIR
import os, uuid


class QuarantineWriter(DataFlowComponent):

    def __init__(self, audit: Audit) -> None:
        super().__init__(audit=audit)

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        # Pass-through — quarantine is called directly via write_rejected_rows()
        return True, [], df

    def write_rejected_rows(self, df: Optional[pd.DataFrame], reason: str) -> None:
        """Persist rejected rows to quarantine folder as CSV."""
        if df is None or df.empty:
            return
        try:
            os.makedirs(QUARANTINE_DIR, exist_ok=True)
            path = os.path.join(QUARANTINE_DIR, f"{reason}_{uuid.uuid4().hex}.csv")
            df_out = df.copy()
            df_out["_quarantine_reason"] = reason
            df_out.to_csv(path, index=False)
            self.audit.track_metrics("quarantine", {"quarantined_count": len(df_out), "reason": reason})
        except Exception as exc:
            self.audit.log_failure(f"QuarantineWriter failed: {exc}")
