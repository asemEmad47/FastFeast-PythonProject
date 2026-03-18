"""
Join — DataFlowComponent.

Called directly by WorkFlow.do_join_task() with two explicit DataFrames.
NOT part of the linear DataFlowTask chain — it operates at orchestration level
because it requires two already-processed dfs from the registry.

Signature: do_task(left_df, right_df) → (ok, errors, merged_df)
"""
from typing import Optional
import pandas as pd
from audit.audit import Audit


class Join:

    def __init__(self, left_on: str, right_on: str, join_type: str, audit: Audit) -> None:
        self.left_on   = left_on
        self.right_on  = right_on
        self.join_type = join_type
        self.audit     = audit

    def do_task(
        self,
        left_df:  pd.DataFrame,
        right_df: pd.DataFrame,
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        try:
            merged = pd.merge(
                left_df,
                right_df,
                left_on  = self.left_on,
                right_on = self.right_on,
                how      = self.join_type,
                suffixes = ("", "_dim"),
            )
            return True, [], merged
        except Exception as exc:
            return False, [f"Join failed ({self.left_on} ← {self.right_on}): {exc}"], left_df
