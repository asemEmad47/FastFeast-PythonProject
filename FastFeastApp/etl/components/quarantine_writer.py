from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry
import json


class QuarantineWriter(DataFlowComponent):
    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)
        self.errors = []

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        errors = []
        bad_rows = data_frame_dict.get("dataframe")
        if bad_rows is None or bad_rows.empty:
            return True, [], data_frame_dict, {}, None

        if "orphan_dim" in bad_rows.columns:
            errors.extend(self._quarantine_orphans(bad_rows, data_frame_dict))
        else:
            errors.extend(self._quarantine_non_orphan(bad_rows, data_frame_dict))

        data_frame_dict["dataframe"] = None

        return True, errors, data_frame_dict, {}, None

    def _quarantine_orphans(self, orphan_df: pd.DataFrame, data_frame_dict: dict) -> list[str]:
        errors = []
        repository = self.registry.get_repository("OrphanRecords")
        if repository is None:
            return ["QuarantineWriter: no repository found for OrphanRecords"]

        fact_table = data_frame_dict.get("target")
        if not fact_table:
            return ["QuarantineWriter: missing target in data_frame_dict"]

        records = []
        for _, row in orphan_df.iterrows():
            dim_name = row.get("orphan_dim")
            fk_col = row.get("orphan_fk")
            fk_value = row.get(fk_col) if fk_col else None

            payload = row.drop(
                labels=["orphan_dim", "orphan_fk"],
                errors="ignore"
            ).to_dict()
      
            records.append({
                "record_payload": json.loads(json.dumps(payload, default=str)), 
                "fact_table": fact_table,
                "source_table": dim_name,
                "orphaned_fk_column": fk_col,
                "orphaned_fk_value": fk_value,
                "quarantined_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),     
                })

        if not repository.add_many(records):
            errors.append("QuarantineWriter: failed to write orphan records to OrphanRecords")

        return errors
    def _quarantine_non_orphan(self, duplicate_df: pd.DataFrame, data_frame_dict: dict) -> list[str]:
        errors = []
        repository = self.registry.get_repository("RejectedRecords")
        if repository is None:
            return ["QuarantineWriter: no repository found for RejectedRecords"]

        batch_source = data_frame_dict.get("dimension") or data_frame_dict.get("target")

        records = []
        for i, (_, row) in enumerate(duplicate_df.iterrows()):
            payload = row.to_dict()
            reason = self.errors[i] if i < len(self.errors) else "Unknown rejection reason"
        records.append({
            "record_payload": json.loads(json.dumps(payload, default=str)),
            "rejected_reason": reason,
            "batch_source": batch_source,
            "rejected_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
        })
        if not repository.add_many(records):
            errors.append("QuarantineWriter: failed to write duplicate records to RejectedRecords")

        return errors
        
    def set_errors(self, errors: list[str]) -> None:
        self.errors = errors