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
        df = data_frame_dict['dataframe']
        if df is None:
            return False, ["DuplicatesLookUp: missing dataframe"], data_frame_dict, {}, None
        
        dimension = data_frame_dict['dimension']
        if dimension is None:
            return False, ["DuplicatesLookUp: missing dimension in data_frame_dict"], data_frame_dict, {}, None
        
        pk = self.registry.get_target_primary_key(dimension)
        if pk is None or self.repository is None:
            return False, ["DuplicatesLookUp: missing pk or repository"], data_frame_dict, {}, None

        repository = self.registry.get_repository(dimension)
        if repository is None:
            return False, [f"DuplicatesLookUp: no repository found for dimension '{dimension}'"], data_frame_dict, {}, None
        
        pk_values = set(df[pk])
        existing_ids = repository.get_existing_ids(pk_values)
        
        if(existing_ids is None):
            return False, ["DuplicatesLookUp: failed to retrieve existing ids from repository"], data_frame_dict, {}, None

        pii_cols = []
        dimentsion_sources = self.registry.get_target_sources(dimension)
        for source in dimentsion_sources:
            source_pii_cols = self.registry.get_source_pii_columns(source)
            if source_pii_cols:
                pii_cols.extend(source_pii_cols)
                
        keep_cols = [col for col in df.columns if col not in pii_cols]
        
        existing_rows = repository.get_columns_by_ids(keep_cols, existing_ids)
        db_df = pd.DataFrame(existing_rows, columns=keep_cols)

        merged = df[keep_cols].merge(db_df, on=keep_cols, how='left', indicator=True)

        duplicate_df = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])
        
        clean_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        metrics = {
            "total_records": len(df),
            "duplicate_count": len(duplicate_df),
            "passed_count": len(clean_df),
        }
        
        errors = []
        for _, row in duplicate_df.iterrows():
            errors.append(f"Duplicate record found with {pk}={row[pk]} on dimension '{dimension}'")

        data_frame_dict["df"] = clean_df
        return True, errors, data_frame_dict, metrics, duplicate_df 