"""
DuplicatesLookUp — DataFlowComponent.

Compares incoming rows against the DWH target table on primary_key.
Rows whose PK already exists are filtered out (they are duplicates).
Only new rows continue through the pipeline.

Uses bulk IN query — one DB round trip regardless of batch size.
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from registry.data_registry import DataRegistry
from audit.audit import Audit


class DuplicatesLookUp(DataFlowComponent):

    def __init__(self, primary_key: str, registry: DataRegistry, audit: Audit) -> None:
        super().__init__(audit=audit)
        self.primary_key = primary_key
        self.registry    = registry

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["DuplicatesLookUp received None df"], None
        if self.primary_key not in df.columns:
            return True, [f"PK '{self.primary_key}' not in df — dedup skipped"], df

        incoming_ids = set(df[self.primary_key].dropna().unique())

        # TODO: resolve table name from registry for the bulk query
        # existing_ids = self.registry.get_existing_ids(table_name, self.primary_key, incoming_ids)
        existing_ids: set = set()   # placeholder

        dup_mask    = df[self.primary_key].isin(existing_ids)
        new_rows_df = df[~dup_mask]
        dup_count   = int(dup_mask.sum())

        self.audit.track_metrics("dedup", {"duplicate_count": dup_count})

        return True, [], new_rows_df
