"""
DuplicatesLookUp — DataFlowComponent (inherits LookUp).
Filters rows whose PK already exists in the DWH.
Updates duplicated_records in metrics.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from registry.data_registry import DataRegistry
from audit.audit import Audit


class DuplicatesLookUp(DataFlowComponent):

    def __init__(
        self,
        primary_key: str,
        table_key:   str,
        registry:    DataRegistry,
        audit:       Audit,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.primary_key = primary_key
        self.table_key   = table_key

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = self.get_df(data_frame_dict)
        if df is None:
            return False, ["DuplicatesLookUp received None df"], data_frame_dict, metrics_dict, bad_rows

        if self.primary_key not in df.columns:
            return True, [f"PK '{self.primary_key}' not in df — dedup skipped"], data_frame_dict, metrics_dict, bad_rows

        incoming_ids = set(df[self.primary_key].dropna().unique())
        existing_ids = self.registry.get_existing_ids(
            self.table_key, self.primary_key, incoming_ids
        )

        dup_mask    = df[self.primary_key].isin(existing_ids)
        dup_rows    = df[dup_mask]
        new_rows_df = df[~dup_mask]
        dup_count   = int(dup_mask.sum())

        metrics_dict["duplicated_records"] += dup_count
        metrics_dict["failed_records"]     += dup_count
        metrics_dict["passed_records"]      = (
            metrics_dict["total_in_records"] - metrics_dict["failed_records"]
        )

        bad_rows = self.append_bad_rows(bad_rows, dup_rows)
        self.set_df(data_frame_dict, new_rows_df)
        self.audit.track_metrics("dedup", {"duplicate_count": dup_count})
        return True, [], data_frame_dict, metrics_dict, bad_rows
