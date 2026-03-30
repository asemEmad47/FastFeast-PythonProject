"""
RejectedRecordsRepository — Concrete repository for RejectedRecords.
Handles records that failed validation and are dead on arrival.
Dead on arrival meaning: null required fields, bad data, failed schema validation.
These records are not retried — they are logged and forgotten.
"""
from __future__ import annotations
from datetime import datetime
import json
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class RejectedRecordsRepository(BaseRepository):

    __table__  = "REJECTED_RECORDS"
    __pk__     = "rejected_id"
    __schema__ = "QUARANTINE"

    def __init__(self, db_manager: DatabaseManager) -> None:
        super().__init__(db_manager)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_rejected_by_id(self, rejected_id: int) -> dict | None:
        """Returns a single rejected record by PK or None if not found."""
        return self.get_by_id(rejected_id)

    def get_all_rejected(self) -> list[dict]:
        """Returns all rejected records."""
        return self.get_all()

    def insert_rejected(self, record: dict, rejection_reason: str, batch_source: str) -> bool:
        """
        Inserts a single rejected record into the quarantine schema.
        record_payload is serialized to JSON string for Snowflake VARIANT storage.
        rejected_id is auto-incremented — not supplied by caller.
        """
        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self._full_table_name()} 
                        (record_payload, rejection_reason, batch_source, rejected_at)
                    VALUES (PARSE_JSON(%s), %s, %s, %s)
                    """,
                    (
                        json.dumps(record),
                        rejection_reason,
                        batch_source,
                        datetime.utcnow().isoformat()
                    )
                )
            return True
        except Exception:
            return False

    def insert_many_rejected(self, records: list[dict], rejection_reason: str, batch_source: str) -> bool:
        """
        Bulk inserts multiple rejected records sharing the same rejection reason.
        Used when an entire file fails schema validation.
        """
        if not records:
            return True
        try:
            rows = [
                (
                    json.dumps(record),
                    rejection_reason,
                    batch_source,
                    datetime.utcnow().isoformat()
                )
                for record in records
            ]
            with self._db.cursor_scope() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO {self._full_table_name()}
                        (record_payload, rejection_reason, batch_source, rejected_at)
                    VALUES (PARSE_JSON(%s), %s, %s, %s)
                    """,
                    rows
                )
            return True
        except Exception:
            return False

    # ── Custom ────────────────────────────────────────────────────────────

    def get_by_batch(self, batch_source: str) -> list[dict]:
        """Returns all rejected records from a specific batch file."""
        return self.get_by_attribute("batch_source", batch_source)

    def get_by_reason(self, rejection_reason: str) -> list[dict]:
        """Returns all rejected records sharing the same rejection reason."""
        return self.get_by_attribute("rejection_reason", rejection_reason)

    def get_by_date_range(self, start: datetime, end: datetime) -> list[dict]:
        """Returns all rejected records within a timestamp range."""
        with self._db.cursor_scope() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {self._full_table_name()}
                WHERE rejected_at BETWEEN %s AND %s
                """,
                (start.isoformat(), end.isoformat())
            )
            return cursor.fetchall()