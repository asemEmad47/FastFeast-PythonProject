from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.lookup.lookup import LookUp
from audit.audit import Audit
from registry.data_registry import DataRegistry
class DuplicatesLookUp(LookUp):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = self.get_df(data_frame_dict)
        if df is None:
            return False, ["DuplicatesLookUp: missing dataframe"], data_frame_dict, {}, None
        
        dimension = data_frame_dict.get("dimension")
        if not dimension:
            return False, ["DuplicatesLookUp: missing dimension"], data_frame_dict, {}, None
        
        repository = self.registry.get_repository(dimension)
        if repository is None:
            return False, [f"DuplicatesLookUp: no repository found for dimension '{dimension}'"], data_frame_dict, {}, None
        
        
