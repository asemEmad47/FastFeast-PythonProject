"""
ValidatorComponent — DataFlowComponent.

Stage 1: SchemaValidator  → hard fail (whole file rejected, chain stops).
Stage 2: RowsValidator    → soft fail (bad rows go to bad_rows, clean df continues).

Updates metrics: null_records, failed_records, passed_records.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from validation.validator_context import ValidatorContext
from validation.schema_validator  import SchemaValidator
from validation.rows_validator    import RowsValidator
from audit.audit import Audit
from registry.data_registry import DataRegistry


class ValidatorComponent(DataFlowComponent):

    def __init__(
        self,
        table_conf: dict,
        audit:      Audit,
        registry:   DataRegistry,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.table_conf = table_conf
        self._context   = ValidatorContext()

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:

        df    = self.get_df(data_frame_dict)
        model = self.registry.get_model_for_file(
            self.table_conf.get("file_name", "")
        )

        # ── Stage 1: Schema — hard fail ────────────────────────────
        self._context.set_validator(SchemaValidator())
        ok, errors, df = self._context.validate(df, model, self.table_conf)
        if not ok:
            metrics_dict["failed_records"] += len(df) if df is not None else 0
            bad_rows = self.append_bad_rows(bad_rows, df)
            return False, errors, data_frame_dict, metrics_dict, bad_rows

        # ── Stage 2: Rows — soft fail ──────────────────────────────
        self._context.set_validator(RowsValidator())
        ok, errors, df = self._context.validate(df, model, self.table_conf)

        # RowsValidator returns cleaned df; rejected rows tracked via errors count
        null_count = sum(1 for e in errors if "Null" in e)
        metrics_dict["null_records"]   += null_count
        metrics_dict["failed_records"] += null_count
        metrics_dict["passed_records"]  = (
            metrics_dict["total_in_records"] - metrics_dict["failed_records"]
        )

        self.set_df(data_frame_dict, df)
        return True, errors, data_frame_dict, metrics_dict, bad_rows
