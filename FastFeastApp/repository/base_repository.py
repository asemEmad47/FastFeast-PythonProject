"""
BaseRepository<T> — Repository Pattern.

Generic base for all table repositories.
Logging via Audit instance — writes to repository/logs/pipeline.log daily.
"""
from __future__ import annotations
import os
from typing import TypeVar, Generic, Optional, Any
from db.database_manager import DatabaseManager
from helpers.logger_builder import build_file_logger


_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
_logger  = build_file_logger("fastfeast.repository", _LOG_DIR)

T = TypeVar("T")


class BaseRepository(Generic[T]):

    __table__:  str = None
    __pk__:     str = None
    __schema__: str = "FASTFEASTDWH"

    def __init__(self, db_manager: DatabaseManager, audit=None) -> None:
        self._db    = db_manager
        self._audit = audit

    def _ctx(self, method: str) -> str:
        return f"[{self.__class__.__name__}.{method}]"

    def _log_success(self, message: str) -> None:
        _logger.info(message)
        if self._audit:
            self._audit.log_success(message)

    def _log_failure(self, message: str) -> None:
        _logger.error(message)
        if self._audit:
            self._audit.log_failure(message)

    def _log_warning(self, message: str) -> None:
        _logger.warning(message)
        if self._audit:
            self._audit.log_failure(message)

    def _full_table_name(self) -> str:
        if self.__schema__:
            return f"{self.__schema__.upper()}.{self.__table__.upper()}"
        return self.__table__.upper()

    # ── Write ──────────────────────────────────────────────────────────────

    def add(self, record: dict) -> bool:
        method = "add"
        try:
            columns      = ", ".join(record.keys())
            placeholders = ", ".join(["%s"] * len(record))
            sql = f"INSERT INTO {self._full_table_name()} ({columns}) VALUES ({placeholders})"
            self._db.execute(sql, tuple(record.values()))
            self._log_success(
                f"{self._ctx(method)} Record inserted into {self._full_table_name()} | "
                f"pk={record.get(self.__pk__, 'unknown')}"
            )
            return True
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to insert record into {self._full_table_name()} | "
                f"Reason: Database rejected the insert statement | "
                f"Raw error: {e}"
            )
            return False

    def add_many(self, records: list[dict]) -> bool:
        method = "add_many"
        if not records:
            self._log_warning(
                f"{self._ctx(method)} Called with empty records list — skipping | "
                f"table={self._full_table_name()}"
            )
            return True
        try:
            columns      = ", ".join(records[0].keys())
            placeholders = ", ".join(["%s"] * len(records[0]))
            sql = f"INSERT INTO {self._full_table_name()} ({columns}) VALUES ({placeholders})"
            with self._db.cursor_scope() as cursor:
                cursor.executemany(sql, [tuple(r.values()) for r in records])
            self._log_success(
                f"{self._ctx(method)} Bulk insert successful | "
                f"table={self._full_table_name()} | records={len(records)}"
            )
            return True
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Bulk insert failed on {self._full_table_name()} | "
                f"Reason: One or more records were rejected by Snowflake | "
                f"records={len(records)} | Raw error: {e}"
            )
            return False

    def upsert_many(self, records: list[dict], pk_column: str = None) -> bool:
        method = "upsert_many"
        if not records:
            self._log_warning(
                f"{self._ctx(method)} Called with empty records list — skipping | "
                f"table={self._full_table_name()}"
            )
            return True

        pk      = pk_column or self.__pk__
        tbl     = self._full_table_name()
        columns = list(records[0].keys())

        update_cols   = [c for c in columns if c != pk]
        set_clause    = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
        insert_cols   = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])
        placeholders  = ", ".join(["%s"] * len(columns))
        values_rows   = ", ".join(["(" + placeholders + ")"] * len(records))

        sql = f"""
            MERGE INTO {tbl} AS target
            USING (
                SELECT {', '.join([f'column{i+1} AS {col}'
                       for i, col in enumerate(columns)])}
                FROM VALUES {values_rows}
            ) AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols})
                VALUES ({insert_values})
        """

        flat_values = [v for r in records for v in r.values()]

        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(sql, flat_values)
            self._log_success(
                f"{self._ctx(method)} Upsert successful | "
                f"table={tbl} | records={len(records)} | pk={pk}"
            )
            return True
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Upsert failed on {tbl} | "
                f"Reason: MERGE statement rejected by Snowflake | "
                f"records={len(records)} | pk={pk} | Raw error: {e}"
            )
            return False

    # ── Read ───────────────────────────────────────────────────────────────

    def get_by_id(self, entity_id: Any) -> Optional[dict]:
        method = "get_by_id"
        try:
            sql  = f"SELECT * FROM {self._full_table_name()} WHERE {self.__pk__} = %s LIMIT 1"
            rows = self._db.execute(sql, (entity_id,))
            return rows[0] if rows else None
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to fetch record from {self._full_table_name()} | "
                f"Reason: Query execution failed | "
                f"pk_value={entity_id} | Raw error: {e}"
            )
            return None

    def get_all(self) -> list[dict]:
        method = "get_all"
        try:
            return self._db.execute(f"SELECT * FROM {self._full_table_name()}")
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to fetch all records from {self._full_table_name()} | "
                f"Reason: Query execution failed | Raw error: {e}"
            )
            return []

    def get_all_ids(self, id_column: str = None) -> set:
        method = "get_all_ids"
        pk     = id_column or self.__pk__
        try:
            rows = self._db.execute(f"SELECT {pk} FROM {self._full_table_name()}")
            return {row[0] for row in rows}
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to fetch IDs from {self._full_table_name()} | "
                f"Reason: Query execution failed | "
                f"id_column={pk} | Raw error: {e}"
            )
            return set()

    def get_by_attribute(self, attr_name: str, attr_value: Any) -> list[dict]:
        method = "get_by_attribute"
        try:
            sql = f"SELECT * FROM {self._full_table_name()} WHERE {attr_name} = %s"
            return self._db.execute(sql, (attr_value,))
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to fetch records from {self._full_table_name()} | "
                f"Reason: Query execution failed | "
                f"filter={attr_name}={attr_value} | Raw error: {e}"
            )
            return []

    def get_existing_ids(self, id_column: str, ids: set) -> set:
        method = "get_existing_ids"
        if not ids:
            return set()
        try:
            placeholders = ", ".join(["%s"] * len(ids))
            sql  = f"SELECT {id_column} FROM {self._full_table_name()} WHERE {id_column} IN ({placeholders})"
            rows = self._db.execute(sql, tuple(ids))
            return {row[0] for row in rows}
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to fetch existing IDs from {self._full_table_name()} | "
                f"Reason: IN query execution failed | "
                f"id_column={id_column} | ids_count={len(ids)} | Raw error: {e}"
            )
            return set()

    # ── Update / Delete ────────────────────────────────────────────────────

    def update(self, entity_id: Any, **kwargs) -> bool:
        method = "update"
        if not kwargs:
            self._log_warning(
                f"{self._ctx(method)} Called with no fields to update — skipping | "
                f"table={self._full_table_name()} | pk_value={entity_id}"
            )
            return False
        try:
            set_clause = ", ".join([f"{k} = %s" for k in kwargs.keys()])
            sql = f"UPDATE {self._full_table_name()} SET {set_clause} WHERE {self.__pk__} = %s"
            self._db.execute(sql, (*kwargs.values(), entity_id))
            self._log_success(
                f"{self._ctx(method)} Record updated in {self._full_table_name()} | "
                f"pk_value={entity_id} | fields={list(kwargs.keys())}"
            )
            return True
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to update record in {self._full_table_name()} | "
                f"Reason: UPDATE statement rejected by Snowflake | "
                f"pk_value={entity_id} | fields={list(kwargs.keys())} | Raw error: {e}"
            )
            return False

    def delete_by_id(self, entity_id: Any) -> bool:
        method = "delete_by_id"
        try:
            sql = f"DELETE FROM {self._full_table_name()} WHERE {self.__pk__} = %s"
            self._db.execute(sql, (entity_id,))
            self._log_success(
                f"{self._ctx(method)} Record hard deleted from {self._full_table_name()} | "
                f"pk_value={entity_id}"
            )
            return True
        except Exception as e:
            self._log_failure(
                f"{self._ctx(method)} Failed to hard delete record from {self._full_table_name()} | "
                f"Reason: DELETE statement rejected by Snowflake | "
                f"pk_value={entity_id} | Raw error: {e}"
            )
            return False