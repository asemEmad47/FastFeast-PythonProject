"""
Transformer — DataFlowComponent.

1. remove_unused_columns()       — keep only columns listed in keep_columns.
2. calculate_aggregated_columns() — compute expressions from aggregated_columns.
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit


class Transformer(DataFlowComponent):

    def __init__(self, table_conf: dict, audit: Audit) -> None:
        super().__init__(audit=audit)
        self.table_conf = table_conf

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["Transformer received None df"], None
        errors = []
        df, e = self.remove_unused_columns(df)
        errors.extend(e)
        df, e = self.calculate_aggregated_columns(df)
        errors.extend(e)
        return True, errors, df

    def remove_unused_columns(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """Drop columns not listed in keep_columns config."""
        keep = self.table_conf.get("keep_columns")
        if not keep:
            return df, []
        existing = [c for c in keep if c in df.columns]
        missing  = [c for c in keep if c not in df.columns]
        errors   = [f"keep_columns: missing column '{c}'" for c in missing]
        return df[existing], errors

    def calculate_aggregated_columns(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """
        Evaluate expressions from aggregated_columns config.
        Each entry: {name: "col_name", expression: "col_a - col_b"}
        TODO: implement expression evaluation per business rules.
        """
        errors = []
        for agg in self.table_conf.get("aggregated_columns", []):
            col_name   = agg.get("name")
            expression = agg.get("expression", "")
            try:
                df[col_name] = df.eval(expression)
            except Exception as exc:
                errors.append(f"Aggregation '{col_name}' failed: {exc}")
        return df, errors
