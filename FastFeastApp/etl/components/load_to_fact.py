import pandas as pd
from typing import Optional
from audit.audit import Audit
from etl.components.data_flow_component import DataFlowComponent
from registry.data_registry import DataRegistry


class LoadToFact(DataFlowComponent):
    
    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        target = data_frame_dict['target']
        df = data_frame_dict['dataframe']
        if target is None:
            return False, ["FactLoad: missing target in data_frame_dict"], data_frame_dict, {}, None
        
        if df is None:
            return False, ["FactLoad: missing dataframe"], data_frame_dict, {}, None
        
        repository = self.registry.get_repository(target)
        if repository is None:
            return False, [f"FactLoad: no repository found for target '{target}'"], data_frame_dict, {}, None
        try:
            data_dicts = df.to_dict(orient='records')
            success = repository.add_many(data_dicts)
            metrics = {
                "records_inserted": len(df)
            }
            if not success:
                return False, ["FactLoad: failed to insert records into repository"], data_frame_dict, {}, None
            return True, [], data_frame_dict, metrics, None
        except Exception as e:
            return False, [f"FactLoad: failed to insert records into repository - {str(e)}"], data_frame_dict, {}, None
        
        