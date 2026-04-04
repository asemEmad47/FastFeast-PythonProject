from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class Join(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)
        self.data_framse_dicts: list[dict] = []

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        errors = []
        
        dimension_groups: dict[str, list[dict]] = {}
        for d in self.data_framse_dicts:
            dimension = d["dimension"]
            if dimension not in dimension_groups:
                dimension_groups[dimension] = []
            dimension_groups[dimension].append(d)

        for dimension, dicts in dimension_groups.items():

            join_configs = self.registry.get_join_config(dimension)
            
            if not join_configs:
                continue
            
            for config in join_configs:

                for right in config["right"]:
                    if "left_key" not in right or "right_key" not in right:
                        errors.append(f"Missing keys in join config for dimension '{dimension}'")
                        return False, errors, data_frame_dict, {}, None
                    
            df_lookup: dict[str, pd.DataFrame] = {}
            
            for d in dicts:
                df_lookup[d["source"]] = d["dataframe"]

            base_source = join_configs[0]["left_table"]
            if base_source not in df_lookup:
                errors.append(f"Base table '{base_source}' not found for dimension '{dimension}'")
                return False, errors, data_frame_dict, {}, None

            merged_df = df_lookup[base_source]

            for config in join_configs:
                join_type = config["type"]

                for right in config["right"]:
                    right_table = right["table"]
                    left_key    = right["left_key"]
                    right_key   = right["right_key"]

                    if right_table not in df_lookup:
                        errors.append(f"Right table '{right_table}' not found for dimension '{dimension}'")
                        return False, errors, data_frame_dict, {}, None

                    merged_df = merged_df.merge(
                        df_lookup[right_table],
                        left_on=left_key,
                        right_on=right_key,
                        how=join_type,
                        suffixes=("", f"_{right_table}")
                    )

            for d in dicts:
                d["dataframe"] = merged_df

            first = dicts[0]
            first["target"] = self.registry.get_target_table_name(first["source"])
            del first["source"]
            for d in dicts[1:]:
                self.data_framse_dicts.remove(d)

        return True, errors, data_frame_dict, {}, None

    def set_data_framse_dict(self, data_framse_dicts: list[dict]) -> None:
        self.data_framse_dicts = data_framse_dicts