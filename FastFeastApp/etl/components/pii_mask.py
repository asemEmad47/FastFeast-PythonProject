"""
PIIMask — DataFlowComponent.
Hashes PII fields before data reaches any storage.
Pass-through for metrics and bad_rows.
"""
from __future__ import annotations
from typing import Optional
import hashlib
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class PIIMask(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        errors = []
        source = data_frame_dict.get("source")
        if not source:
            errors.append("Missing 'source' in data_frame_dict")
            return False, errors, data_frame_dict, {}, None
        
        pii_fields = self.registry.get_pii_columns(source)
        if not pii_fields:
            return True, errors, data_frame_dict, {}, None
        
        df = data_frame_dict.get("dataframe")
        if df is None:
            errors.append("Missing 'dataframe' in data_frame_dict")
            return False, errors, data_frame_dict, {}, None
        
        for field in pii_fields:
            if field in df.columns:
                df[field] = df[field].apply(self._hash_pii)
            else:
                errors.append(f"PII field '{field}' not found in dataframe columns")

        data_frame_dict["dataframe"] = df
        return True, errors, data_frame_dict, {}, None
    
    def _hash_pii(self,val):
        if pd.isnull(val):
            return val
        
        byte_data = str(val).encode()
        return hashlib.sha256(byte_data).hexdigest()
