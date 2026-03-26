"""
BaseRepository<T> — Repository Pattern.

Generic base for all table repositories.
Provides CRUD + bulk operations via Snowflake native connector.
Every concrete repository inherits this and sets:
    __table__    = "FACT_TICKETS"         (Snowflake table name)
    __pk__       = "ticket_id"            (primary key column name)
    __schema__   = "ANALYTICS"            (schema override, optional)
"""
from __future__ import annotations
from typing import TypeVar, Generic, Optional, Any
from db.database_manager import DatabaseManager

T = TypeVar("T")


class BaseRepository(Generic[T]):

    __table__:  str = None   # set by every concrete subclass
    __pk__:     str = None   # primary key column name
    __schema__: str = "FASTFEASTDWH"   # optional schema override

    def __init__(self, db_manager: DatabaseManager) -> None:
        self._db = db_manager

    def _full_table_name(self) -> str:
        """
        Resolves to SCHEMA.TABLE or just TABLE if no schema override.
        Snowflake always uppercases unquoted identifiers so we enforce it here.
        """
        if self.__schema__:
            return f"{self.__schema__.upper()}.{self.__table__.upper()}"
        return self.__table__.upper()

    # ── Write ──────────────────────────────────────────────────────────────

    def add(self, record: dict) -> bool:
        """Insert a single record."""
        try:
            columns  = ", ".join(record.keys())
            placeholders = ", ".join(["%s"] * len(record))
            sql = f"INSERT INTO {self._full_table_name()} ({columns}) VALUES ({placeholders})"
            self._db.execute(sql, tuple(record.values()))
            return True
        except Exception:
            return False

    def add_many(self, records: list[dict]) -> bool:
        """
        Bulk insert a list of dicts.
        All dicts must have identical keys.
        Uses executemany for one round trip.
        """
        if not records:
            return True
        try:
            columns      = ", ".join(records[0].keys())
            placeholders = ", ".join(["%s"] * len(records[0]))
            sql = f"INSERT INTO {self._full_table_name()} ({columns}) VALUES ({placeholders})"
            with self._db.cursor_scope() as cursor:
                cursor.executemany(sql, [tuple(r.values()) for r in records])
            return True
        except Exception:
            return False

    def upsert_many(self, records: list[dict], pk_column: str = None) -> bool:
        """
        Idempotent bulk upsert using Snowflake MERGE.
        Re-processing the same file will not duplicate records.
        pk_column defaults to __pk__ if not provided.
        """
        if not records:
            return True

        pk  = pk_column or self.__pk__
        tbl = self._full_table_name()
        columns = list(records[0].keys())

        # Build SET clause for WHEN MATCHED (all columns except PK)
        update_cols = [c for c in columns if c != pk]
        set_clause  = ", ".join([f"target.{c} = source.{c}" for c in update_cols])

        # Build INSERT clause for WHEN NOT MATCHED
        insert_cols   = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])

        # Build the VALUES block for the staging CTE
        placeholders = ", ".join(["%s"] * len(columns))
        values_rows  = ", ".join(["(" + placeholders + ")"] * len(records))

        sql = f"""
            MERGE INTO {tbl} AS target
            USING (
                SELECT {', '.join([f'column{i+1} AS {col}'
                       for i, col in enumerate(columns)])}
                FROM VALUES {values_rows}
            ) AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED THEN UPDATE SET
                {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols})
                VALUES ({insert_values})
        """

        flat_values = [v for r in records for v in r.values()]

        try:
            with self._db.cursor_scope() as cursor:
                cursor.execute(sql, flat_values)
            return True
        except Exception:
            return False

    # ── Read ───────────────────────────────────────────────────────────────

    def get_by_id(self, entity_id: Any) -> Optional[dict]:
        """Fetch a single row by primary key. Returns a dict or None."""
        sql  = f"SELECT * FROM {self._full_table_name()} WHERE {self.__pk__} = %s LIMIT 1"
        rows = self._db.execute(sql, (entity_id,))
        return rows[0] if rows else None

    def get_all(self) -> list[dict]:
        """Full table scan — use carefully on large fact tables."""
        sql = f"SELECT * FROM {self._full_table_name()}"
        return self._db.execute(sql)

    def get_all_ids(self, id_column: str = None) -> set:
        """
        Return a set of all PK values.
        Used for cache warming and orphan detection.
        """
        pk  = id_column or self.__pk__
        sql = f"SELECT {pk} FROM {self._full_table_name()}"
        rows = self._db.execute(sql)
        return {row[0] for row in rows}

    def get_by_attribute(self, attr_name: str, attr_value: Any) -> list[dict]:
        """Fetch all rows matching a single column value."""
        sql  = f"SELECT * FROM {self._full_table_name()} WHERE {attr_name} = %s"
        return self._db.execute(sql, (attr_value,))

    def get_existing_ids(self, id_column: str, ids: set) -> set:
        """
        Bulk IN query — one round trip.
        Used by validation layer for orphan reference checks.
        Returns only the IDs that actually exist in the table.
        """
        if not ids:
            return set()
        placeholders = ", ".join(["%s"] * len(ids))
        sql  = f"SELECT {id_column} FROM {self._full_table_name()} WHERE {id_column} IN ({placeholders})"
        rows = self._db.execute(sql, tuple(ids))
        return {row[0] for row in rows}

    # ── Update / Delete ────────────────────────────────────────────────────

    def update(self, entity_id: Any, **kwargs) -> bool:
        """Update specific columns on a single row by PK."""
        if not kwargs:
            return False
        try:
            set_clause = ", ".join([f"{k} = %s" for k in kwargs.keys()])
            sql = f"UPDATE {self._full_table_name()} SET {set_clause} WHERE {self.__pk__} = %s"
            self._db.execute(sql, (*kwargs.values(), entity_id))
            return True
        except Exception:
            return False

    def delete_by_id(self, entity_id: Any) -> bool:
        """Hard delete by primary key."""
        try:
            sql = f"DELETE FROM {self._full_table_name()} WHERE {self.__pk__} = %s"
            self._db.execute(sql, (entity_id,))
            return True
        except Exception:
            return False