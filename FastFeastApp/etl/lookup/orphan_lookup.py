from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.lookup.lookup import LookUp
from audit.audit import Audit
from registry.data_registry import DataRegistry

class OrphanLookUp(LookUp):
    def __init__(self, audit: Audit, registry: DataRegistry):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        df = data_frame_dict.get('dataframe')
        
        if df is None or df.empty:
            return False, ["OrphanLookUp: Dataframe is empty"], data_frame_dict, {}, None

        current_df = df.copy()
        target_table = data_frame_dict.get("target")
        fact_dimensions = self.registry.get_fact_dimension_sources(target_table)

        if not fact_dimensions:
            return True, [], data_frame_dict, {"status": "no dimensions to check"}, None

        orphan_reasons = pd.Series("", index=current_df.index)
        orphan_dims = pd.Series("", index=current_df.index) 
        orphan_fks = pd.Series("", index=current_df.index)
        is_orphan = pd.Series(False, index=current_df.index)

        for dim_source in fact_dimensions:
            dim_name = dim_source.get("dim")
            fk = dim_source.get("fk")

            if not dim_name or not fk or not isinstance(fk, str):
                continue
            
            if fk not in current_df.columns:
                continue
            
            non_null_mask = current_df[fk].notna()
            if not non_null_mask.any():
                continue

            fact_fk_values = set(current_df.loc[non_null_mask, fk])
            dim_repo = self.registry.get_repository(dim_name)
            if dim_repo:
                existing_ids = dim_repo.get_existing_ids(fact_fk_values)
                
                fact_fk_values_str = {str(x) for x in fact_fk_values}
                existing_ids_str = {str(x) for x in existing_ids}
                
                orphans_str = fact_fk_values_str - existing_ids_str
                
                if orphans_str:
                    row_is_orphan_this_dim = non_null_mask & current_df[fk].astype(str).isin(orphans_str)
                    
                    if row_is_orphan_this_dim.any():
                        is_orphan |= row_is_orphan_this_dim
                        
                        orphan_reasons.loc[row_is_orphan_this_dim] += f"Missing {dim_name} Key ({fk}); "
                        orphan_dims.loc[row_is_orphan_this_dim] += f"{dim_name}|"
                        orphan_fks.loc[row_is_orphan_this_dim] += f"{fk}|"

        clean_df = current_df[~is_orphan].reset_index(drop=True)
        combined_orphans = current_df[is_orphan].copy()
        
        if not combined_orphans.empty:
            combined_orphans['orphan_dim'] = orphan_dims[is_orphan].str.rstrip("|")
            combined_orphans['orphan_fk'] = orphan_fks[is_orphan].str.rstrip("|")
            orphan_errors_list = orphan_reasons[is_orphan].tolist()
        else:
            orphan_errors_list = []

        data_frame_dict['dataframe'] = clean_df

        metrics = {
            "total_records": len(df),
            "orphan_count": len(combined_orphans),
            "passed_count": len(clean_df)
        }

        print(f"OrphanLookUp Metrics: {metrics}")

        return True, orphan_errors_list, data_frame_dict, metrics, combined_orphans if not combined_orphans.empty else None