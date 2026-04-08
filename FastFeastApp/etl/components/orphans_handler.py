from __future__ import annotations
from collections import defaultdict
from typing import Optional
import pandas as pd
import json
from etl.components.data_flow_component import DataFlowComponent


class OrphansHandler(DataFlowComponent):
    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:

        orphan_repo = self.registry.get_repository("OrphanRecords")
        if orphan_repo is None:
            return False, ["OrphansHandler: no repository found for 'OrphanRecords'"], data_frame_dict, {}, None

        all_orphans = orphan_repo.get_all()

        if not all_orphans:
            return True, [], data_frame_dict, {}, None

        grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

        for orphan in all_orphans:
            fact_table = orphan.get("fact_table")
            payload = orphan.get("record_payload")
            if isinstance(payload, str):
                payload = json.loads(payload)

            pk_col = self.registry.get_target_primary_key(fact_table)
            ticket_id = payload.get(pk_col)

            grouped[fact_table][ticket_id].append({
                "quarantine_id": orphan.get("quarantine_id"),
                "source_table": orphan.get("source_table"),
                "orphaned_fk_column": orphan.get("orphaned_fk_column"),
                "orphaned_fk_value": str(orphan.get("orphaned_fk_value")),
                "record_payload": payload,
            })

        errors = []

        for fact_table, tickets in grouped.items():
            fact_repo = self.registry.get_repository(fact_table)
            fully_resolved_payloads: list[dict] = []
            fully_resolved_quarantine_ids: list = []

            if fact_repo is None:
                errors.append(f"OrphansHandler: no repository found for fact_table='{fact_table}'")
                continue

            for ticket_id, rows in tickets.items():
                cleared_ids: list = []
                unresolved_ct: int = 0

                for row in rows:
                    dim_table = row["source_table"]
                    fk_value = row["orphaned_fk_value"]
                    q_id = row["quarantine_id"]

                    dim_repo = self.registry.get_repository(dim_table)
                    if dim_repo is None:
                        unresolved_ct += 1
                        continue

                    exists = dim_repo.get_existing_ids({fk_value})

                    if exists:
                        cleared_ids.append(q_id)
                    else:
                        unresolved_ct += 1

                if unresolved_ct == 0:
                    for r in rows:
                        fully_resolved_payloads.append(r["record_payload"])
                    fully_resolved_quarantine_ids.extend(cleared_ids)
                else:
                    if cleared_ids:
                        orphan_repo.delete_many(cleared_ids)

            if fully_resolved_payloads:
                if fact_repo.add_many(fully_resolved_payloads):
                    orphan_repo.delete_many(fully_resolved_quarantine_ids)
                else:
                    msg = (
                        f"Failed to insert {len(fully_resolved_payloads)} resolved record(s) "
                        f"into '{fact_table}'. Quarantine rows NOT deleted."
                    )
                    errors.append(msg)

        return True, errors, data_frame_dict, {}, None