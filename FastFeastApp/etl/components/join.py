"""
Join — DataFlowComponent.

Owns data_framse_dicts: List[dict] — the list of all source data_frame_dicts
loaded by before_join_components. Set by DataFlowTask via set_data_framse_dict().

Merges all source DataFrames sequentially using join_configs from pipeline.yaml.
Result replaces data_frame_dict["dataframe"] for the after_join stage.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class Join(DataFlowComponent):

    def __init__(
        self,
        join_configs: list[dict],
        audit:        Audit,
        registry:     DataRegistry = None,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.join_configs       = join_configs          # from pipeline.yaml joins section
        self.data_framse_dicts: list[dict] = []         # set by DataFlowTask

    def set_data_framse_dict(self, data_framse_dicts: list[dict]) -> None:
        """Called by DataFlowTask after before_join stage completes."""
        self.data_framse_dicts = data_framse_dicts

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        """
        Left df comes from data_frame_dict (primary source, already processed).
        Right dfs come from self.data_framse_dicts by matching source key.
        """
        merged_df = self.get_df(data_frame_dict)
        if merged_df is None:
            return False, ["Join: primary df is None"], data_frame_dict, metrics_dict, bad_rows

        errors = []

        for cfg in self.join_configs:
            left_on    = cfg.get("left_on")
            right_key  = cfg.get("right_key")     # source file key e.g. "orders"
            right_on   = cfg.get("right_on")
            join_type  = cfg.get("type", "left")

            # Find matching right df from data_framse_dicts by source key
            right_dict = next(
                (d for d in self.data_framse_dicts if d.get("source") == right_key),
                None,
            )
            if right_dict is None:
                msg = f"Join: no df found for source '{right_key}' — skipping"
                errors.append(msg)
                self.audit.log_failure(msg)
                continue

            right_df = self.get_df(right_dict)
            if right_df is None:
                errors.append(f"Join: df for '{right_key}' is None — skipping")
                continue

            try:
                merged_df = pd.merge(
                    merged_df,
                    right_df,
                    left_on  = left_on,
                    right_on = right_on,
                    how      = join_type,
                    suffixes = ("", f"_{right_key}"),
                )
            except Exception as exc:
                return (
                    False,
                    [f"Join failed ({left_on} ← {right_key}.{right_on}): {exc}"],
                    data_frame_dict, metrics_dict, bad_rows,
                )

        self.audit.track_metrics("join", {"output_rows": len(merged_df)})
        self.set_df(data_frame_dict, merged_df)
        return True, errors, data_frame_dict, metrics_dict, bad_rows
