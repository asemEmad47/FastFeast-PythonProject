"""
ReadFromCSV — DataFlowComponent.
Reads a CSV file and populates data_frame_dict["dataframe"].
Sets total_in_records in metrics.
"""
from __future__ import annotations
import pandas as pd
from etl.components.read_from_source import ReadFromSource

class ReadFromCSV(ReadFromSource):

    def _read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)
