"""
BaseRepository<T> — Repository Pattern.

Generic base for all table repositories.
Errors logged to repository/logs/pipeline.log daily via log_error helper.
"""
from __future__ import annotations
import os
from typing import TypeVar, Generic, Optional, Any
from registry.data_registry import DataRegistry
from db.database_manager import DatabaseManager
from helpers.logger_builder import build_file_logger, log_error


_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
_logger  = build_file_logger("fastfeast.repository", _LOG_DIR)

T = TypeVar("T")


class BaseRepository(Generic[T]):

    def __init__(self, db_manager: DatabaseManager, registry: DataRegistry, table_key: str, audit=None) -> None:
        self._db       = db_manager
        self._audit    = audit
        self._registry = registry
        self.__table__  = registry.get_target_table_name(table_key).upper()
        self.__pk__     = registry.get_target_primary_key(table_key)
        self.__schema__ = registry.get_target_schema(table_key).upper()

    def _ctx(self, method: str) -> str:
        return f"[{self.__class__.__name__}.{method}]"

    def _full_table_name(self) -> str:
        if self.__schema__:
            return f"{self.__schema__}.{self.__table__}"
        return self.__table__

    # ── Write ──────────────────────────────────────────────────────────────

    def add_many(self, records: list[dict]) -> bool:
        method = "add_many"
        if not records:
            return True
        try:
            columns      = ", ".join(records[0].keys())
            placeholders = ", ".join(["%s"] * len(records[0]))
            sql = f"INSERT INTO {self._full_table_name()} ({columns}) VALUES ({placeholders})"
            with self._db.cursor_scope() as cursor:
                cursor.executemany(sql, [tuple(r.values()) for r in records])
            return True
        except Exception as e:
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Bulk insert failed on {self._full_table_name()} | "
                f"Reason: One or more records were rejected by Snowflake | "
                f"records={len(records)} | Raw error: {e}"
            )
            return False

    def upsert_many(self, records: list[dict]) -> bool:
        method = "upsert_many"
        if not records:
            return True

        pk      = self.__pk__
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
            return True
        except Exception as e:
            log_error(_logger, self._audit,
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
            log_error(_logger, self._audit,
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
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to fetch all records from {self._full_table_name()} | "
                f"Reason: Query execution failed | Raw error: {e}"
            )
            return []

    def get_all_ids(self) -> set:
        method = "get_all_ids"
        pk = self.__pk__
        try:
            rows = self._db.execute(f"SELECT {pk} FROM {self._full_table_name()}")
            return {row[0] for row in rows}
        except Exception as e:
            log_error(_logger, self._audit,
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
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to fetch records from {self._full_table_name()} | "
                f"Reason: Query execution failed | "
                f"filter={attr_name}={attr_value} | Raw error: {e}"
            )
            return []

    def get_existing_ids(self, ids: set) -> set:
        method = "get_existing_ids"
        if not ids:
            return set()
        try:
            placeholders = ", ".join(["%s"] * len(ids))
            sql  = f"SELECT {self.__pk__} FROM {self._full_table_name()} WHERE {self.__pk__} IN ({placeholders})"
            rows = self._db.execute(sql, tuple(ids))
            return {row[0] for row in rows}
        except Exception as e:
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to fetch existing IDs from {self._full_table_name()} | "
                f"Reason: IN query execution failed | "
                f"id_column={self.__pk__} | ids_count={len(ids)} | Raw error: {e}"
            )
            return set()

    # ── Update / Delete ────────────────────────────────────────────────────

    def update(self, entity_id: Any, **kwargs) -> bool:
        method = "update"
        if not kwargs:
            return False
        try:
            set_clause = ", ".join([f"{k} = %s" for k in kwargs.keys()])
            sql = f"UPDATE {self._full_table_name()} SET {set_clause} WHERE {self.__pk__} = %s"
            self._db.execute(sql, (*kwargs.values(), entity_id))
            return True
        except Exception as e:
            log_error(_logger, self._audit,
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
            return True
        except Exception as e:
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to hard delete record from {self._full_table_name()} | "
                f"Reason: DELETE statement rejected by Snowflake | "
                f"pk_value={entity_id} | Raw error: {e}"
            )
            return False
        
    def get_columns(self, columns: list[str]) -> list[tuple]:
        """
        Retrieves only specific columns from the table.
        
        Example:
            repo.get_columns(["agent_id", "agent_name", "is_active"])
        """
        method = "get_columns"
        if not columns:
            return []
        try:
            col_clause = ", ".join(columns)
            sql  = f"SELECT {col_clause} FROM {self.__table__}"
            return self._db.execute(sql)
        except Exception as e:
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to fetch columns from {self.__table__} | "
                f"Reason: Query execution failed | "
                f"columns={columns} | Raw error: {e}"
            )
            return []

    def delete_by_filters(self, **filters) -> bool:
        """
        Deletes rows matching ALL supplied filter conditions.
        Filters are combined with AND.

        Example:
            repo.delete_by_filters(is_active=False, team_id=10)
        """
        method = "delete_by_filters"
        if not filters:
            return False
        try:
            where_clause = " AND ".join([f"{k} = %s" for k in filters.keys()])
            sql  = f"DELETE FROM {self.__table__} WHERE {where_clause}"
            self._db.execute(sql, tuple(filters.values()))
            return True
        except Exception as e:
            log_error(_logger, self._audit,
                f"{self._ctx(method)} Failed to delete records from {self.__table__} | "
                f"Reason: DELETE statement rejected by Snowflake | "
                f"filters={filters} | Raw error: {e}"
            )
            return False