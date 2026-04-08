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
from helpers.aggregation_helper import AggregationHelper

class Transformer(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        
        errors = []
        dimension = data_frame_dict.get("dimension")
        df = data_frame_dict.get("dataframe")
        if dimension is None or df is None:
            errors.append("Missing 'dimension' or 'dataframe' in data_frame_dict")
            return False, errors, data_frame_dict, {}, None

        success, errors, df = self._calculate_aggregated_columns(df, dimension)
        if not success:
            return False, errors, data_frame_dict, {}, None

        df = self._rename_columns(df, dimension)
        success, df = self._remove_unused_columns(df, dimension)
        if not success:
            errors.append(f"Failed to remove unused columns for dimension '{dimension}'")
            return False, errors, data_frame_dict, {}, None

        df = df.loc[:, ~df.columns.duplicated()]
        data_frame_dict["dataframe"] = df
        
        return True, errors, data_frame_dict, {}, None
    
    def _remove_unused_columns(self, df: pd.DataFrame, dimension: str) -> tuple[bool, pd.DataFrame]:
        keep_cols = self.registry.get_dimension_columns(dimension)
        
        if not keep_cols:
            return False, df
        
        missing = [col for col in keep_cols if col not in df.columns]
        if missing:
            print(f"[Transformer] Missing columns in DataFrame: {missing}")
            print(f"[Transformer] Available columns: {df.columns.tolist()}")
            return False, df
        
        return True, df[keep_cols]
    
    def _calculate_aggregated_columns(self, df: pd.DataFrame, dimension: str) -> tuple[bool, list[str], pd.DataFrame]:
        aggregated_columns = self.registry.get_aggregated_columns(dimension)

        if not aggregated_columns:
            return True, [], df

        return AggregationHelper.apply(df, aggregated_columns)

    def _rename_columns(self, df: pd.DataFrame, dimension: str) -> pd.DataFrame:
        rename_map = self.registry.get_renamed_columns(dimension)
        if rename_map:
            existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
            if existing_rename:
                df = df.rename(columns=existing_rename)
        return df