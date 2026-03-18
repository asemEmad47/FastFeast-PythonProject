"""
OrphansHandler — DataFlowComponent.

Checks every FK in table_conf['foreign_keys'] against the referenced dim table.
Rows whose FK value is not found in the dim are quarantined (orphans).
Clean rows continue through the pipeline.

Two-tier lookup strategy (from design):
  - Small/stable dim tables  → cached set in DataRegistry (lookup_cached)
  - Large/growing tables     → bulk IN query  (get_existing_ids)
"""
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from etl.components.quarantine_writer import QuarantineWriter
from registry.data_registry import DataRegistry
from audit.audit import Audit


class OrphansHandler(DataFlowComponent):

    def __init__(self, foreign_keys: dict, registry: DataRegistry, audit: Audit) -> None:
        super().__init__(audit=audit)
        self.foreign_keys = foreign_keys   # {fk_col: "dim_table.pk_col"}
        self.registry     = registry
        self._quarantine  = QuarantineWriter(audit=audit)

    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["OrphansHandler received None df"], None

        errors = []
        clean_df = df.copy()

        for fk_col, ref in self.foreign_keys.items():
            dim_table, pk_col = ref.split(".")

            if fk_col not in clean_df.columns:
                errors.append(f"OrphansHandler: FK column '{fk_col}' not in df")
                continue

            incoming_ids = set(clean_df[fk_col].dropna().unique())
            existing_ids = self.registry.get_existing_ids(dim_table, pk_col, incoming_ids)

            orphan_mask = ~clean_df[fk_col].isin(existing_ids)
            orphans     = clean_df[orphan_mask]
            clean_df    = clean_df[~orphan_mask]

            if not orphans.empty:
                reason = f"orphan_fk:{fk_col}"
                self._quarantine.write_rejected_rows(orphans, reason)
                self.audit.track_metrics("orphan_check", {"orphan_count": len(orphans), "fk": fk_col})
                errors.append(f"Orphans quarantined on '{fk_col}': {len(orphans)} rows")

        return True, errors, clean_df
