from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

class ReadFromSource(ABC):

    def do_task(self,data_frame_dict: dict,) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        metrics_dict = {
            "total_in_records": 0,
            "passed_records": 0,
            "failed_records": 0,
            "duplicated_records": 0,
            "orphaned_records": 0,
            "null_records": 0,
        }

        bad_rows: Optional[pd.DataFrame] = None

        try:
            file_path = data_frame_dict.get("source")

            if not file_path:
                print("Missing 'source' in data_frame_dict")
                return False, [], data_frame_dict, metrics_dict, bad_rows
            
            df = self._read(file_path)

            metrics_dict["total_in_records"] = len(df)
            metrics_dict["passed_records"] = len(df)

            data_frame_dict["dataframe"] = df

            return True, [], data_frame_dict, metrics_dict, bad_rows
        
        except Exception as exc:
            return (
                False,
                [f"{self.__class__.__name__} failed [{self.file_path}]: {exc}"],
                data_frame_dict,
                metrics_dict,
                bad_rows
            )
    
    @abstractmethod
    def _read(self) -> pd.DataFrame:
        pass