"""
DataFlowTask — Runs three sequential stages for one DWH table.

Attributes (per UML):
  data_framse_dicts : List[dict]               — one dict per source file
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
          Results stored back in data_framse_dicts.

Stage 2 — join: merge all source dfs using data_framse_dicts.
          Primary source dict flows as the left side.

Stage 3 — after_join: Transformer → Orphans → Dedup → SCD → LoadToTarget.
          LoadToTarget returns the short (bool, errors, df) tuple — handled separately.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.task import Task
from audit.audit import Audit
from registry.data_registry import DataRegistry
from etl.components.data_flow_component import DataFlowComponent


class DataFlowTask(Task):

    def __init__(
        self,
        audit:                   Audit,
        registry:                DataRegistry,
        data_framse_dicts:       list[dict]            = None,
        before_join_components:  list[list[DataFlowComponent]] = None,
        join_task                                       = None,
        after_join_components:   list[DataFlowComponent] = None,
    ) -> None:
        self.audit                   = audit
        self.registry                = registry
        self.data_framse_dicts       = data_framse_dicts      or []
        self.before_join_components  = before_join_components or []  # list of lists (one per source)
        self.join_task               = join_task
        self.after_join_components   = after_join_components  or []

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC — called by WorkFlow.orchestrate()
    # ══════════════════════════════════════════════════════════════════

    def do_task(self) -> tuple[bool, list[str]]:
        all_errors: list[str] = []

        # ── Stage 1: before-join — per source file ──────────────────
        for i, component_chain in enumerate(self.before_join_components):
            if i >= len(self.data_framse_dicts):
                break

            data_frame_dict = self.data_framse_dicts[i]
            metrics_dict    = DataFlowComponent.make_metrics()
            bad_rows        = None

            for component in component_chain:
                ok, errors, data_frame_dict, metrics_dict, bad_rows = component.do_task(
                    data_frame_dict, metrics_dict, bad_rows
                )
                all_errors.extend(errors)
                if not ok:
                    self.audit.log_failure(
                        f"Before-join failed [{data_frame_dict.get('source')}]: {errors}"
                    )
                    break   # stop this source's chain, continue to next source

            # Store metrics per source
            self.audit.track_metrics(
                data_frame_dict.get("source", f"source_{i}"), metrics_dict
            )
            # Write back updated dict
            self.data_framse_dicts[i] = data_frame_dict

        # ── Stage 2: join ────────────────────────────────────────────
        if self.join_task is not None:
            self.join_task.set_data_framse_dict(self.data_framse_dicts)

            # Primary source is always the first dict
            primary_dict = self.data_framse_dicts[0] if self.data_framse_dicts else {}
            metrics_dict = DataFlowComponent.make_metrics()
            bad_rows     = None

            ok, errors, primary_dict, metrics_dict, bad_rows = self.join_task.do_task(
                primary_dict, metrics_dict, bad_rows
            )
            all_errors.extend(errors)
            if not ok:
                self.audit.log_failure(f"Join stage failed: {errors}")
                return False, all_errors

            # Put merged result as the single dict for after_join
            self.data_framse_dicts[0] = primary_dict

        # ── Stage 3: after-join ───────────────────────────────────────
        working_dict = self.data_framse_dicts[0] if self.data_framse_dicts else {}
        metrics_dict = DataFlowComponent.make_metrics()
        bad_rows     = None

        after_components      = self.after_join_components
        load_target_component = None

        # Separate LoadToTarget (last step — different return signature per UML)
        from etl.components.load_to_target import LoadToTarget
        if after_components and isinstance(after_components[-1], LoadToTarget):
            load_target_component = after_components[-1]
            after_components      = after_components[:-1]

        for component in after_components:
            ok, errors, working_dict, metrics_dict, bad_rows = component.do_task(
                working_dict, metrics_dict, bad_rows
            )
            all_errors.extend(errors)
            if not ok:
                self.audit.log_failure(f"After-join failed: {errors}")
                return False, all_errors

        # LoadToTarget — terminal component, short return signature
        if load_target_component is not None:
            ok, errors, _ = load_target_component.do_task(
                working_dict, metrics_dict, bad_rows
            )
            all_errors.extend(errors)
            if not ok:
                return False, all_errors

        self.audit.track_metrics(
            working_dict.get("dimension", "unknown"), metrics_dict
        )
        return True, all_errors
