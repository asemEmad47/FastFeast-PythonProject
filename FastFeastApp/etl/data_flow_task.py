"""
DataFlowTask — Runs a linear chain of DataFlowComponents.

Each component receives the df produced by the previous one.
Returns (ok, errors, final_df).

set_input_df() injects a pre-built df for Phase 3 (after-join tasks)
so the chain skips re-reading from source.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.task import Task
from audit.audit import Audit


class DataFlowTask(Task):

    def __init__(self, components: list, source: str, audit: Audit) -> None:
        self.components  = components   # List[DataFlowComponent]
        self.source      = source
        self.audit       = audit
        self._input_df: Optional[pd.DataFrame] = None

    def set_input_df(self, df: pd.DataFrame) -> None:
        """Inject starting df — used in Phase 3 post-join tasks."""
        self._input_df = df

    def do_task(self) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        current_df  = self._input_df
        all_errors: list[str] = []

        for component in self.components:
            ok, errors, current_df = component.do_task(current_df)
            if errors:
                all_errors.extend(errors)
            if not ok:
                return False, all_errors, current_df

        return True, all_errors, current_df
