"""
OrphansHandler — DataFlowComponent.
Orchestrates orphan checking across ALL foreign keys in table_conf.
Creates one OrphanLookUp per FK and runs them in sequence.

UML methods:
  still_orphan(dimName, rec_id)     → checks if a record is still an orphan
  remove_from_orphans(attribute)    → marks orphan as resolved in orphan repo
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from etl.lookup.orphan_lookup import OrphanLookUp
from registry.data_registry import DataRegistry
from audit.audit import Audit


class OrphansHandler(DataFlowComponent):

    def __init__(
        self,
        foreign_keys: dict,     # {fk_col: "DimTable.pk_col"}
        registry:     DataRegistry,
        audit:        Audit,
    ) -> None:
        super().__init__(audit=audit, registry=registry)
        self.foreign_keys = foreign_keys

    def do_task(
        self,
        data_frame_dict: dict,
        metrics_dict:    dict,
        bad_rows:        Optional[pd.DataFrame],
    ) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        errors = []

        for fk_col, ref in self.foreign_keys.items():
            dim_table, pk_col = ref.split(".")

            lookup = OrphanLookUp(
                fk_column = fk_col,
                dim_table = dim_table,
                pk_column = pk_col,
                registry  = self.registry,
                audit     = self.audit,
            )
            ok, errs, data_frame_dict, metrics_dict, bad_rows = lookup.do_task(
                data_frame_dict, metrics_dict, bad_rows
            )
            errors.extend(errs)
            if not ok:
                return False, errors, data_frame_dict, metrics_dict, bad_rows

        return True, errors, data_frame_dict, metrics_dict, bad_rows

    def still_orphan(self, dim_name: str, rec_id) -> bool:
        """
        Check if a previously quarantined record is still an orphan
        by verifying the FK value now exists in the dim table.
        Returns True if still orphan (FK not found), False if resolved.
        """
        existing = self.registry.get_existing_ids(dim_name, dim_name, {rec_id})
        return rec_id not in existing

    def remove_from_orphans(self, attribute: str) -> bool:
        """
        Mark orphan records resolved in the orphan repository
        after the late-arriving dimension finally arrives.
        """
        orphan_repo = self.registry.get_repository("OrphanRepo")
        if orphan_repo is None:
            return False
        try:
            orphan_repo.mark_resolved_by_fk(attribute)
            return True
        except Exception as exc:
            self.audit.log_failure(f"remove_from_orphans failed [{attribute}]: {exc}")
            return False
