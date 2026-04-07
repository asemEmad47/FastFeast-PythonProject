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
            
            # --- Print Before ---
            print("\n" + "="*40)
            print("ORPHAN LOOKUP: DICT BEFORE")
            print(f"Target: {data_frame_dict.get('target')}")
            if df is not None:
                print(df.head(3))
            print("="*40)

            if df is None or df.empty:
                return False, ["OrphanLookUp: Dataframe is empty"], data_frame_dict, {}, None

            current_df = df.copy()
            target_table = data_frame_dict.get("target")
            fact_dimensions = self.registry.get_fact_dimension_sources(target_table)

            if not fact_dimensions:
                return True, [], data_frame_dict, {"status": "no dimensions to check"}, None

            orphan_reasons = pd.Series("", index=current_df.index)
            is_orphan = pd.Series(False, index=current_df.index)

            for dim_source in fact_dimensions:
                dim_name = dim_source.get("dim")
                fk = dim_source.get("fk")

                # 1. Handle Missing Config
                if not dim_name or not fk:
                    continue

                # 2. Fix 'unhashable type: list'
                # If fk is a list (composite key), we need to ensure all columns exist
                if isinstance(fk, list):
                    missing_cols = [c for c in fk if c not in current_df.columns]
                    if missing_cols:
                        continue
                    # For now, skip orphan logic for composite keys if repo doesn't support them
                    # or implement composite logic here.
                    continue 
                
                # 3. Handle Single String FK
                if fk not in current_df.columns:
                    continue
                
                non_null_mask = current_df[fk].notna()
                fact_fk_values = set(current_df.loc[non_null_mask, fk])
                
                if not fact_fk_values:
                    continue

                dim_repo = self.registry.get_repository(dim_name)
                if dim_repo:
                    existing_ids = dim_repo.get_existing_ids(fact_fk_values)
                    orphans = fact_fk_values - existing_ids
                    
                    if orphans:
                        row_is_orphan = non_null_mask & current_df[fk].isin(orphans)
                        is_orphan |= row_is_orphan
                        orphan_reasons.loc[row_is_orphan] += f"Missing {dim_name} Key ({fk}); "

            # 4. Finalize Clean vs Orphan
            clean_df = current_df[~is_orphan].reset_index(drop=True)
            combined_orphans = current_df[is_orphan].copy()
            orphan_errors_list = orphan_reasons[is_orphan].tolist()

            data_frame_dict['dataframe'] = clean_df

            # --- Print After ---
            print("\n" + "="*40)
            print("ORPHAN LOOKUP: DICT AFTER")
            print(f"Clean Rows: {len(clean_df)} | Orphans: {len(combined_orphans)}")
            if not combined_orphans.empty:
                print(f"Sample Reason: {orphan_errors_list[0]}")
            print("="*40 + "\n")

            metrics = {
                "total_records": len(df),
                "orphan_count": len(combined_orphans),
                "passed_count": len(clean_df)
            }

            return True, orphan_errors_list, data_frame_dict, metrics, combined_orphans if not combined_orphans.empty else None