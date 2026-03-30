"""
OrphanLookUp — DataFlowComponent (inherits LookUp).
Checks a single FK column against the DWH dim table.
Rows with unresolved FK values go to bad_rows.
Updates orphaned_records in metrics.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from registry.data_registry import DataRegistry
from audit.audit import Audit


class OrphanLookUp(DataFlowComponent):

    def __init__(
        self,
        fk_column:  str,
        dim_table:  str,
        pk_column:  str,
        registry:   DataRegistry,
        audit:      Audit,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.fk_column = fk_column
        self.dim_table = dim_table
        self.pk_column = pk_column

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = self.get_df(data_frame_dict)
        if df is None:
            return False, ["OrphanLookUp received None df"], data_frame_dict, metrics_dict, bad_rows

        if self.fk_column not in df.columns:
            return True, [f"FK '{self.fk_column}' not in df — orphan check skipped"], data_frame_dict, metrics_dict, bad_rows

        incoming_ids = set(df[self.fk_column].dropna().unique())
        existing_ids = self.registry.get_existing_ids(
            self.dim_table, self.pk_column, incoming_ids
        )

        orphan_mask = ~df[self.fk_column].isin(existing_ids)
        orphan_rows = df[orphan_mask]
        clean_df    = df[~orphan_mask]
        orphan_count = int(orphan_mask.sum())

        metrics_dict["orphaned_records"] += orphan_count
        metrics_dict["failed_records"]   += orphan_count
        metrics_dict["passed_records"]    = (
            metrics_dict["total_in_records"] - metrics_dict["failed_records"]
        )

        bad_rows = self.append_bad_rows(bad_rows, orphan_rows)
        self.set_df(data_frame_dict, clean_df)
        self.audit.track_metrics("orphan_check", {
            "orphan_count": orphan_count,
            "fk": self.fk_column,
        })
        return True, [], data_frame_dict, metrics_dict, bad_rows
