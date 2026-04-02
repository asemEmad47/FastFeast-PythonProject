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


class OrphanRecordsRepository(BaseRepository):

    def __init__(self, db_manager, registry, audit=None):
        super().__init__(db_manager, registry, "OrphanRecords", audit=audit)

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
                        quarantined_at
                    )
                    VALUES (PARSE_JSON(%s), %s, %s, %s, %s, %s)
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
                        quarantined_at
                    )
                    VALUES (PARSE_JSON(%s), %s, %s, %s, %s, %s)
                    """,
                    rows
                )
            return True
        except Exception:
            return False

    def delete_by_id(self, quarantine_id: int) -> bool:
        """
        Deletes a quarantined record by its primary key.
        Returns True if deletion succeeds, otherwise False.
        """
        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self._full_table_name()}
                    WHERE quarantine_id = %s
                    """,
                    (quarantine_id,)
                )
            return True
        except Exception:
            return False


    def delete_by_orphaned_fk(
        self,
        orphaned_fk_column: str,
        orphaned_fk_value:  str
    ) -> bool:
        """
        Deletes quarantined records matching a specific orphaned FK column and value.
        Returns True if deletion succeeds, otherwise False.
        """
        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self._full_table_name()}
                    WHERE orphaned_fk_column = %s
                      AND orphaned_fk_value  = %s
                    """,
                    (
                        orphaned_fk_column,
                        str(orphaned_fk_value)
                    )
                )
            return True
        except Exception:
            return False

    # ── Custom ────────────────────────────────────────────────────────────

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