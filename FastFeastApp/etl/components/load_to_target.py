"""
LoadToTarget — DataFlowComponent.
Writes a DataFrame to the DWH target table via repo.upsert_many().

Note: per UML, LoadToTarget returns (bool, List[str], data_frame)
— not the full 5-tuple — because it is the terminal component in
the after_join chain and metrics/bad_rows are already finalized.
DataFlowTask handles this special return signature for the last step.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class LoadToTarget(DataFlowComponent):

    def __init__(
        self,
        source:    str,
        repo,
        registry:  DataRegistry,
        audit:     Audit,
        pk_column: str = None,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.source    = source
        self.repo      = repo
        self.pk_column = pk_column

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        """
        Returns (bool, List[str], data_frame) — terminal signature per UML.
        """
        df = self.get_df(data_frame_dict)
        if df is None or df.empty:
            return True, [f"LoadToTarget [{self.source}]: nothing to load"], df
        try:
            records = df.to_dict(orient="records")
            ok      = self.repo.upsert_many(records, pk_column=self.pk_column)
            if ok:
                self.audit.track_metrics(self.source, {"upserted": len(records)})
            return ok, [], df
        except Exception as exc:
            return False, [f"LoadToTarget failed [{self.source}]: {exc}"], df
