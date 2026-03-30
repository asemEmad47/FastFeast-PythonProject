"""
ReadFromCSV — DataFlowComponent.
Reads a CSV file and populates data_frame_dict["dataframe"].
Sets total_in_records in metrics.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class ReadFromCSV(DataFlowComponent):

    def __init__(self, file_path: str, audit: Audit, registry: DataRegistry) -> None:
        super().__init__(audit=audit, registry=registry)
        self.file_path = file_path

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        try:
            df = pd.read_csv(self.file_path)
            metrics_dict["total_in_records"] = len(df)
            self.set_df(data_frame_dict, df)
            return True, [], data_frame_dict, metrics_dict, bad_rows
        except Exception as exc:
            return (
                False,
                [f"ReadFromCSV failed [{self.file_path}]: {exc}"],
                data_frame_dict, metrics_dict, bad_rows,
            )
