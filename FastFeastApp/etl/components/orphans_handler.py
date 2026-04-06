from __future__ import annotations
from collections import defaultdict
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from registry.data_registry import DataRegistry
from audit.audit import Audit


class OrphansHandler(DataFlowComponent):
    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        orphan_table = self.registry.get_orphan_table_name()
        if not orphan_table:
            return False, ["OrphansHandler: orphan table name not found in registry"], data_frame_dict, {}, None

        repository = self.registry.get_repository(orphan_table)
        if repository is None:
            return False, [f"OrphansHandler: no repository found for '{orphan_table}'"], data_frame_dict, {}, None

        orphan_records = repository.get_all()
        if orphan_records is None:
            return False, ["OrphansHandler: failed to retrieve orphan records from repository"], data_frame_dict, {}, None

        if not orphan_records:
            return True, [], data_frame_dict, {}, None

        errors = []
        orphans_by_table = defaultdict(list)

        for orphan in orphan_records:
            source_table = orphan.get("source_table")
            if source_table:
                orphans_by_table[source_table].append(orphan)
            else:
                errors.append(f"OrphansHandler: missing source_table in record — {orphan}")

        for source_table, records in orphans_by_table.items():
            dim_repository = self.registry.get_repository(source_table)
            if dim_repository is None:
                errors.append(f"OrphansHandler: no repository found for '{source_table}'")
                continue

            fact_name = records[0].get("fact_table")
            fact_repository = self.registry.get_repository(fact_name)
            if fact_repository is None:
                errors.append(f"OrphansHandler: no repository found for fact table '{fact_name}'")
                continue

            fk_values = {r.get("orphaned_fk_value") for r in records if r.get("orphaned_fk_value")}
            existing_ids = dim_repository.get_existing_ids(fk_values)
            still_orphan = fk_values - existing_ids

            resolved_fact_rows = []
            resolved_quarantine_ids = set()

            for orphan in records:
                quarantine_id = orphan.get("quarantine_id")
                fk_value = orphan.get("orphaned_fk_value")
                fk_column = orphan.get("orphaned_fk_column")
                fact_record = orphan.get("record_payload")

                if not all([quarantine_id, fk_value, fk_column, fact_record]):
                    errors.append(f"OrphansHandler: incomplete orphan record — {orphan}")
                    continue

                if fk_value in still_orphan:
                    errors.append(
                        f"OrphansHandler: fk '{fk_column}={fk_value}' "
                        f"still missing in '{source_table}' — kept in quarantine"
                    )
                    continue

                resolved_fact_rows.append(fact_record)
                resolved_quarantine_ids.add(quarantine_id)

            if not resolved_fact_rows:
                continue

            if fact_repository.add_many(resolved_fact_rows):
                if not repository.delete_many(resolved_quarantine_ids):
                    errors.append(
                        f"OrphansHandler: inserted {len(resolved_fact_rows)} resolved records into '{fact_name}' "
                        f"but failed to delete them from quarantine"
                    )
            else:
                errors.append(
                    f"OrphansHandler: failed to insert resolved records into '{fact_name}' "
                    f"— kept in quarantine"
                )

        return True, errors, data_frame_dict, {}, None