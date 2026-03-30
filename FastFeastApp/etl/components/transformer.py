"""
Transformer — DataFlowComponent.
1. remove_unused_columns() — keep only columns in keep_columns.
2. calculate_aggregated_columns() — evaluate expressions from aggregated_columns.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class Transformer(DataFlowComponent):

    def __init__(
        self,
        table_conf: dict,
        audit:      Audit,
        registry:   DataRegistry = None,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.table_conf = table_conf

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = self.get_df(data_frame_dict)
        if df is None:
            return False, ["Transformer received None df"], data_frame_dict, metrics_dict, bad_rows

        errors = []
        df, e = self.remove_unused_columns(df)
        errors.extend(e)
        df, e = self.calculate_aggregated_columns(df)
        errors.extend(e)

        self.set_df(data_frame_dict, df)
        return True, errors, data_frame_dict, metrics_dict, bad_rows

    def remove_unused_columns(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, list[str]]:
        keep     = self.table_conf.get("keep_columns")
        if not keep:
            return df, []
        existing = [c for c in keep if c in df.columns]
        missing  = [c for c in keep if c not in df.columns]
        errors   = [f"keep_columns: missing column '{c}'" for c in missing]
        return df[existing], errors

    def calculate_aggregated_columns(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, list[str]]:
        errors = []
        for agg in self.table_conf.get("aggregated_columns", []):
            col_name   = agg.get("name")
            expression = agg.get("expression", "")
            try:
                df[col_name] = df.eval(expression)
            except Exception as exc:
                errors.append(f"Aggregation '{col_name}' failed: {exc}")
        return df, errors
