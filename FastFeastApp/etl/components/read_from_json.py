"""
ReadFromJSON — DataFlowComponent. Reads a JSON (records) file and returns a raw DataFrame.
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit


class ReadFromJSON(DataFlowComponent):

    def __init__(self, file_name: str, registry, parser, audit: Optional[Audit] = None) -> None:
        super().__init__(audit=audit, source=file_name)
        self.file_name = file_name
        self.registry  = registry
        self.parser    = parser

    def do_task(self, df: Optional[pd.DataFrame] = None) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        try:
            result = pd.read_json(self.file_name, orient="records")
            return True, [], result
        except Exception as exc:
            return False, [f"ReadFromJSON failed [{self.file_name}]: {exc}"], None
