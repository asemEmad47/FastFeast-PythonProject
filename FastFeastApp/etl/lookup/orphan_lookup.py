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
            return False, ["OrphanLookUp: fact dataframe is empty or missing"], data_frame_dict, {}, None

        fact_dimensions = self.registry.get_fact_dimension_sources(df["target"])
        if not fact_dimensions:
            return False, [f"OrphanLookUp: no fact-dimension sources found for '{df['target']}'"], data_frame_dict, {}, None

        total_records = len(df)
        all_orphan_frames = []
        errors = []

        for dim_source in fact_dimensions:
            dim_name = dim_source.get("dim")
            fk = dim_source.get("fk")

            if not dim_name or not fk:
                errors.append(f"OrphanLookUp: skipping incomplete dimension source: {dim_source}")
                continue

            if isinstance(fk, list):
                continue

            dim_repo = self.registry.get_repository(dim_name)
            if dim_repo is None:
                errors.append(f"OrphanLookUp: repository for '{dim_name}' not found in registry")
                continue

            fact_fk_values = set(df[fk].dropna())
            if not fact_fk_values:
                continue

            existing_ids = dim_repo.get_existing_ids(fact_fk_values)
            orphan_values = fact_fk_values - existing_ids

            if orphan_values:
                orphan_rows = df[df[fk].isin(orphan_values)].copy()
                orphan_rows["orphan_dim"] = dim_name
                orphan_rows["orphan_fk"] = fk
                all_orphan_frames.append(orphan_rows)

        combined_orphans = pd.concat(all_orphan_frames, ignore_index=False) if all_orphan_frames else None

        if combined_orphans is not None:
            orphan_indices = combined_orphans.index.unique()
            df = df.drop(index=orphan_indices).reset_index(drop=True)
            data_frame_dict['dataframe'] = df
            combined_orphans = combined_orphans.reset_index(drop=True)

        orphan_count = len(combined_orphans) if combined_orphans is not None else 0

        metrics = {
            "total_records": total_records,
            "orphan_count": orphan_count,
            "passed_count": total_records - orphan_count,
        }

        return True, errors, data_frame_dict, metrics, combined_orphans