from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.lookup.lookup import LookUp
from audit.audit import Audit
from registry.data_registry import DataRegistry


class OrphanLookUp(LookUp):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = data_frame_dict['dataframe']
        if df is None:
            return False, ["OrphanLookUp: missing dataframe"], data_frame_dict, {}, None
        
        orphan_table = self.registry.get_orphan_table_name()
        if not orphan_table:
            return False, ["OrphanLookUp: orphan table name not found in registry"], data_frame_dict, {}, None

        pk = self.registry.get_target_primary_key(orphan_table)
        if pk is None or self.repository is None:   
            return False, ["OrphanLookUp: missing pk or repository"], data_frame_dict, {}, None
        
        repository = self.registry.get_repository(orphan_table)
        if repository is None:
            return False, [f"OrphanLookUp: no repository found for dimension '{orphan_table}'"], data_frame_dict, {}, None
        
        pk_values = set(df[pk])
        existing_ids = repository.get_existing_ids(pk_values)
        
        if(existing_ids is None):
            return False, ["OrphanLookUp: failed to retrieve existing ids from repository"], data_frame_dict, {}, None
        
        orphan_ids = pk_values - existing_ids
        orphan_df = df[df[pk].isin(orphan_ids)]
        
        metrics = {
            "total_records": len(df),
            "orphan_count": len(orphan_df),
            "passed_count": len(df) - len(orphan_df),
        }
        errors = []
        for orphan_id in orphan_ids:
            errors.append(f"OrphanLookUp: record with {pk}={orphan_id} does not exist in repository")
        
        return True, errors, data_frame_dict, metrics, orphan_df
        
