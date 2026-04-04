from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from etl.lookup.orphan_lookup import OrphanLookUp
from registry.data_registry import DataRegistry
from audit.audit import Audit


class OrphansHandler(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        orphan_table = self.registry.get_orphan_table_name()
        if not orphan_table:
            return False, ["OrphansHandler: orphan table name not found in registry"], data_frame_dict, {}, None
        
        
        
        
        
    def still_orphan(self, dim_name: str, ids: set) -> bool:

        existing = self.registry.get_existing_ids(dim_name, dim_name, ids)
        return not ids.issubset(existing)

    def remove_from_orphans(self, attribute: str) -> bool:

        orphan_repo = self.registry.get_repository("OrphanRepo")
        if orphan_repo is None:
            return False
        try:
            orphan_repo.mark_resolved_by_fk(attribute)
            return True
        except Exception as exc:
            self.audit.log_failure(f"remove_from_orphans failed [{attribute}]: {exc}")
            return False
