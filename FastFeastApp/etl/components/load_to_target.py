from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class LoadToTarget(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        print("in load to target ")
        target = data_frame_dict['target']
        df = data_frame_dict['dataframe']
        dimension = data_frame_dict['dimension']
        if target is None:
            return False, ["LoadToTarget: missing target in data_frame_dict"], data_frame_dict, {}, None
        
        if df is None:
            return False, ["LoadToTarget: missing dataframe"], data_frame_dict, {}, None
        
        repository = self.registry.get_repository(target)
        if repository is None:
            return False, [f"LoadToTarget: no repository found for dimension '{dimension}'"], data_frame_dict, {}, None
        
        try:
            data_dicts = df.to_dict(orient='records')
            success = repository.upsert_many(data_dicts)
            metrics = {
                "records_inserted": len(df)
            }
            if not success:
                return False, ["LoadToTarget: failed to insert records into dimension " + dimension], data_frame_dict, {}, None
            return True, [], data_frame_dict, metrics, None
        except Exception as e:
            return False, [f"LoadToTarget: failed to insert records into dimension {dimension} - {str(e)}"], data_frame_dict, {}, None