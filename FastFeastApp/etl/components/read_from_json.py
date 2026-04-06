"""
ReadFromJSON — DataFlowComponent.
Reads a JSON (records) file and populates data_frame_dict["dataframe"].
Sets total_in_records in metrics.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.read_from_source import ReadFromSource


class ReadFromJSON(ReadFromSource):

    def _read(self, file_path: str) -> pd.DataFrame:
        return pd.read_json(file_path, orient="records")  
