"""""
DataFlowTask — Runs three sequential stages for one DWH table.

Attributes (per UML):
  dataframe_dicts : List[dict]               — one dict per source file
  audit             : Audit
  registry          : DataRegistry
  before_join_components : List[DataFlowComponent]
  join_task         : Join | None
  after_join_components  : List[DataFlowComponent]

Each data_frame_dict shape:
  {"dataframe": pd.DataFrame, "dimension": str, "source": str}

metrics_dict is created once and passed by reference through every component.

Stage 1 — before_join: process each source file independently.
          Each source runs its own Read → Validate → PIIMask chain.
          Results stored back in dataframe_dicts.

Stage 2 — join: merge all source dfs using dataframe_dicts.
          Primary source dict flows as the left side.

Stage 3 — after_join: Transformer → Orphans → Dedup → SCD → LoadToTarget.
          LoadToTarget returns the short (bool, errors, df) tuple — handled separately.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd

class DataFlowTask():

    def __init__(
        self,
        audit ,
        registry,
        before_join_components = None,
        join_task = None,
        after_join_components = None,
    ) :
        self.audit = audit
        self.registry = registry
        self.before_join_components = before_join_components or {}
        self.join_task = join_task
        self.after_join_components = after_join_components or []

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC — called by WorkFlow.orchestrate()
    # ══════════════════════════════════════════════════════════════════

    def do_task(self, dataframe_dicts) -> tuple[bool, list[str]]:
        all_errors: list[str] = []

        # ── Stage 1: before-join ──────────────────
        for i, data_dict in enumerate(dataframe_dicts):
            source = data_dict.get("source")

            if not source:
                self.audit.log_failure("Missing source")
                continue

            chain = self.before_join_components.get(source, [])

            if not chain:
                self.audit.log_failure(f"No components for {source}")
                continue

            for comp in chain:
                ok, errors, data_dict, metrics, bad_rows = comp.do_task(data_dict)
                all_errors.extend(errors)

                if not ok:
                    self.audit.log_failure(f"Before-join failed [{source}]")
                    break

            dataframe_dicts[i] = data_dict
            self.audit.track_metrics(source, metrics)

        # ── Stage 2: join ────────────────────────────────────────────
        if self.join_task and dataframe_dicts:

            ok, errors, result_dicts, metrics, bad_rows = self.join_task.do_task(dataframe_dicts)
            all_errors.extend(errors)

            if not ok:
                self.audit.log_failure(f"Join failed: {errors}")
                return False, all_errors

            # replace with joined result(s)
            dataframe_dicts = result_dicts

       # ── Stage 3: after-join ───────────────────────────────────────
        if not dataframe_dicts:
            return True, all_errors

        for i, data_dict in enumerate(dataframe_dicts):
            dimension = data_dict["dimension"]

            working = data_dict

            for comp in self.after_join_components:
                ok, errors, working, metrics, bad_rows = comp.do_task(working)
                all_errors.extend(errors)

                if not ok:
                    self.audit.log_failure(f"After-join failed [{dimension}]")
                    return False, all_errors

            dataframe_dicts[i] = working
            self.audit.track_metrics(dimension, metrics)

        return True, all_errors