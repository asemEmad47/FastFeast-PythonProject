"""
FactLoadTask — Phase 4: load dimensions first, then facts.
Dimensions must exist in the DWH before facts reference their surrogate keys.
"""
from __future__ import annotations
from etl.task import Task
from audit.audit import Audit
from registry.data_registry import DataRegistry


class FactLoadTask(Task):

    def __init__(
        self,
        dim_components:  list,
        fact_components: list,
        registry:        DataRegistry,
        audit:           Audit,
    ) -> None:
        self.dim_components  = dim_components
        self.fact_components = fact_components
        self.registry        = registry
        self.audit           = audit

    def do_task(self) -> tuple[bool, list[str]]:

        # ── Dimension load ──────────────────────────────────────────
        for component in self.dim_components:
            df = self.registry.get_df(component.source)
            if df is None:
                continue
            ok, errors, _ = component.do_task(df)
            self.audit.track_metrics(component.source, {"loaded": ok})
            if not ok:
                return False, [f"Dim load failed: {component.source} — {errors}"]

        # ── Fact load ───────────────────────────────────────────────
        for component in self.fact_components:
            df = self.registry.get_df(component.source)
            if df is None:
                continue
            ok, errors, _ = component.do_task(df)
            self.audit.track_metrics(component.source, {"loaded": ok})
            if not ok:
                return False, [f"Fact load failed: {component.source} — {errors}"]

        return True, []
