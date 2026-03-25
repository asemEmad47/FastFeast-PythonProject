"""
DatabaseManager — Singleton Pattern.

One instance, one connection.
All repositories call execute() or cursor_scope() — never manage connections themselves.
"""
from __future__ import annotations
from contextlib import contextmanager
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from config.settings import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE
)


class DatabaseManager:

    _instance: DatabaseManager | None = None

    def __new__(cls) -> DatabaseManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connection = cls._instance._create_connection()
        return cls._instance

    def _create_connection(self) -> SnowflakeConnection:
        return snowflake.connector.connect(
            account   = SNOWFLAKE_ACCOUNT,
            user      = SNOWFLAKE_USER,
            password  = SNOWFLAKE_PASSWORD,
            database  = SNOWFLAKE_DATABASE,
            schema    = SNOWFLAKE_SCHEMA,
            warehouse = SNOWFLAKE_WAREHOUSE,
            role      = SNOWFLAKE_ROLE,
            client_session_keep_alive = True
        )
    
    def _is_alive(self) -> bool: # Work around to check whether the connection is alive or not to avoid silent timeout
        try:
            self._connection.cursor().execute("SELECT 1")
            return True
        except Exception:
            return False
        
    def _ensure_connection(self) -> None:
        if self._connection.is_closed() or not self._is_alive():
            self._connection = self._create_connection()

    @property
    def connection(self) -> SnowflakeConnection:
        return self._connection

    @contextmanager
    def cursor_scope(self) -> SnowflakeCursor:
        """
        Provide a managed cursor — closes automatically after use.
        Use this for all query execution across repositories.
        """
        self._ensure_connection() # Important to ensure connectivity with snowflake before init the cursor
        cursor = self._connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(self, sql: str, params: tuple = None) -> list:
        """
        Fire-and-forget execution for single statements.
        Returns all fetched rows.
        """
        with self.cursor_scope() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def close(self) -> None:
        if self._connection and not self._connection.is_closed():
            self._connection.close()