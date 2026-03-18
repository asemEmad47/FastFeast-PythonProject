"""
DataFlowComponent — Abstract base for every pipeline component.

Contract: do_task(df) → (ok, errors, df)
  One df in, one df out. All side-effects happen internally.
"""
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from audit.audit import Audit


class DataFlowComponent(ABC):

    def __init__(self, audit: Audit, source: str = "") -> None:
        self.audit  = audit
        self.source = source

    @abstractmethod
    def do_task(
        self, df: Optional[pd.DataFrame]
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        """
        Process df. Return (success, error_messages, result_df).
        Hard failure (ok=False) → DataFlowTask chain stops.
        Soft failure (bad rows quarantined) → return ok=True with cleaned df.
        """
        ...
