"""
OrpahnRecordsRepostiroy — Concrete repository for QuarantineRecords.
Handles records that are valid but contain unresolvable foreign key references.
These records are alive and waiting — they are retried when the dimension eventually arrives.
"""
from __future__ import annotations
from datetime import datetime
import json
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class OrpahnRecordsRepostiroy(BaseRepository):

    __table__  = "ORPHAN_RECORDS"
    __pk__     = "quarantine_id"
    __schema__ = "QUARANTINE"

    def __init__(self, db_manager: DatabaseManager) -> None:
        super().__init__(db_manager)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_quarantine_by_id(self, quarantine_id: int) -> dict | None:
        """Returns a single quarantined record by PK or None if not found."""
        return self.get_by_id(quarantine_id)

    def get_all_quarantined(self) -> list[dict]:
        """Returns all quarantined records regardless of resolved status."""
        return self.get_all()

    def insert_quarantined(
        self,
        record:            dict,
        source_table:      str,
        source_file:       str,
        orphaned_fk_column: str,
        orphaned_fk_value:  str
    ) -> bool:
        """
        Inserts a single quarantined record.
        record_payload is serialized to JSON for Snowflake VARIANT storage.
        resolved defaults to FALSE, resolved_at defaults to NULL.
        quarantine_id is auto-incremented — not supplied by caller.
        """
        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self._full_table_name()} (
                        record_payload,
                        source_table,
                        source_file,
                        orphaned_fk_column,
                        orphaned_fk_value,
                        quarantined_at,
                        resolved,
                        resolved_at
                    )
                    VALUES (PARSE_JSON(%s), %s, %s, %s, %s, %s, FALSE, NULL)
                    """,
                    (
                        json.dumps(record),
                        source_table.upper(),
                        source_file,
                        orphaned_fk_column,
                        str(orphaned_fk_value),
                        datetime.utcnow().isoformat()
                    )
                )
            return True
        except Exception:
            return False

    def insert_many_quarantined(
        self,
        records:            list[dict],
        source_table:       str,
        source_file:        str,
        orphaned_fk_column: str,
        orphaned_fk_values: list
    ) -> bool:
        """
        Bulk inserts multiple quarantined records sharing the same
        orphaned FK column but potentially different FK values.
        records and orphaned_fk_values must be the same length and order.
        """
        if not records:
            return True
        try:
            rows = [
                (
                    json.dumps(record),
                    source_table.upper(),
                    source_file,
                    orphaned_fk_column,
                    str(orphaned_fk_value),
                    datetime.utcnow().isoformat()
                )
                for record, orphaned_fk_value in zip(records, orphaned_fk_values)
            ]
            with self._db.cursor_scope() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO {self._full_table_name()} (
                        record_payload,
                        source_table,
                        source_file,
                        orphaned_fk_column,
                        orphaned_fk_value,
                        quarantined_at,
                        resolved,
                        resolved_at
                    )
                    VALUES (PARSE_JSON(%s), %s, %s, %s, %s, %s, FALSE, NULL)
                    """,
                    rows
                )
            return True
        except Exception:
            return False

    def mark_resolved(self, quarantine_id: int) -> bool:
        """
        Marks a single quarantined record as resolved.
        Called after the late-arriving dimension record finally lands
        and the quarantined record is successfully reprocessed.
        """
        return self.update(
            quarantine_id,
            resolved=True,
            resolved_at=datetime.utcnow().isoformat()
        )

    # ── Custom ────────────────────────────────────────────────────────────

    def get_unresolved(self) -> list[dict]:
        """
        Returns all unresolved quarantined records.
        This is the primary query for the retry mechanism —
        called after every new batch load to attempt reprocessing.
        """
        return self.get_by_attribute("resolved", False)

    def get_unresolved_by_fk(self, orphaned_fk_column: str) -> list[dict]:
        """
        Returns all unresolved records with a specific orphaned FK column.
        Used to target retry attempts after a specific dimension loads.
        For example — after customers batch arrives, retry all records
        where orphaned_fk_column = 'customer_id'.
        """
        with self._db.cursor_scope() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {self._full_table_name()}
                WHERE orphaned_fk_column = %s
                AND resolved = FALSE
                """,
                (orphaned_fk_column,)
            )
            return cursor.fetchall()

    def get_by_source_table(self, source_table: str) -> list[dict]:
        """Returns all quarantined records destined for a specific table."""
        return self.get_by_attribute("source_table", source_table.upper())

    def get_by_source_file(self, source_file: str) -> list[dict]:
        """Returns all quarantined records from a specific source file."""
        return self.get_by_attribute("source_file", source_file)

    def get_by_date_range(self, start: datetime, end: datetime) -> list[dict]:
        """Returns all quarantined records within a timestamp range."""
        with self._db.cursor_scope() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {self._full_table_name()}
                WHERE quarantined_at BETWEEN %s AND %s
                """,
                (start.isoformat(), end.isoformat())
            )
            return cursor.fetchall()