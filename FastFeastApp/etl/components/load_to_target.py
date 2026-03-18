"""
LoadToTarget — DataFlowComponent.
Writes a DataFrame to the target DWH table via BaseRepository.add_many().
Used by FactLoadTask for both dim and fact tables.
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class LoadToTarget(DataFlowComponent):

    def __init__(self, source: str, repo, registry: DataRegistry, audit: Audit) -> None:
        super().__init__(audit=audit, source=source)
        self.repo     = repo
        self.registry = registry

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None or df.empty:
            return True, ["LoadToTarget: nothing to load"], df
        try:
            records = df.to_dict(orient="records")
            ok = self.repo.add_many(records)
            return ok, [], df
        except Exception as exc:
            return False, [f"LoadToTarget failed [{self.source}]: {exc}"], df
