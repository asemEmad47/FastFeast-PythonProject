"""
ValidatorComponent — DataFlowComponent.

Stage 1: SchemaValidator  → hard fail (whole file quarantined, chain stops).
Stage 2: RowsValidator    → soft fail (bad rows quarantined, clean df continues).
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from etl.components.quarantine_writer import QuarantineWriter
from validation.validator_context import ValidatorContext
from validation.schema_validator import SchemaValidator
from validation.rows_validator import RowsValidator
from audit.audit import Audit


class ValidatorComponent(DataFlowComponent):

    def __init__(self, parser, registry, audit: Audit, table_conf: dict) -> None:
        super().__init__(audit=audit)
        self.parser      = parser
        self.registry    = registry
        self.table_conf  = table_conf
        self._context    = ValidatorContext()
        self._quarantine = QuarantineWriter(audit=audit)

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        model = self.registry.get_model_for_file(self.table_conf.get("file_name", ""))

        # Stage 1 — schema (hard fail)
        self._context.set_validator(SchemaValidator())
        ok, errors, df = self._context.validate(df, model, self.table_conf)
        if not ok:
            self._quarantine.write_rejected_rows(df, reason="schema_fail")
            return False, errors, None

        # Stage 2 — rows (soft fail — RowsValidator returns cleaned df)
        self._context.set_validator(RowsValidator())
        ok, errors, df = self._context.validate(df, model, self.table_conf)

        self.audit.track_metrics("row_validation", {"rejected_count": len(errors), "errors": errors})
        return True, errors, df
