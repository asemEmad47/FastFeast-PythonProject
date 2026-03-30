"""
DataFlowComponent — Abstract base for every pipeline component.

Contract:
    do_task(data_frame_dict, metrics_dict, bad_rows)
        → (bool, List[str], data_frame_dict, metrics_dict, bad_rows)

data_frame_dict shape:
    {
        "dataframe": pd.DataFrame,
        "dimension": str,    # DWH target table key  e.g. "AgentsDim"
        "source":    str,    # source file key        e.g. "agents"
    }

metrics_dict shape — passed by reference, mutated in place:
    {
        "total_in_records":   0,
        "passed_records":     0,
        "failed_records":     0,
        "duplicated_records": 0,
        "orphaned_records":   0,
        "null_records":       0,
    }

bad_rows: pd.DataFrame | None — accumulates rejected rows across all components.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from audit.audit import Audit
from registry.data_registry import DataRegistry


class DataFlowComponent(ABC):

    def __init__(self, audit: Audit, registry: DataRegistry = None) -> None:
        self.audit    = audit
        self.registry = registry

    @abstractmethod
    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        """
        Process data_frame_dict["dataframe"].
        Mutate metrics_dict in place.
        Accumulate rejected rows in bad_rows.
        Return (ok, errors, data_frame_dict, metrics_dict, bad_rows).
        """
        ...

    # ── Shared static helpers ─────────────────────────────────────────
    @staticmethod
    def make_metrics() -> dict:
        """Return a fresh zeroed metrics dict."""
        return {
            "total_in_records":   0,
            "passed_records":     0,
            "failed_records":     0,
            "duplicated_records": 0,
            "orphaned_records":   0,
            "null_records":       0,
        }

    @staticmethod
    def get_df(data_frame_dict: dict) -> Optional[pd.DataFrame]:
        return data_frame_dict.get("dataframe")

    @staticmethod
    def set_df(data_frame_dict: dict, df: pd.DataFrame) -> dict:
        data_frame_dict["dataframe"] = df
        return data_frame_dict

    @staticmethod
    def append_bad_rows(
        bad_rows: Optional[pd.DataFrame],
        new_bad:  pd.DataFrame,
    ) -> pd.DataFrame:
        """Safely concatenate new rejected rows into bad_rows accumulator."""
        if new_bad is None or new_bad.empty:
            return bad_rows
        if bad_rows is None or bad_rows.empty:
            return new_bad.copy()
        return pd.concat([bad_rows, new_bad], ignore_index=True)
