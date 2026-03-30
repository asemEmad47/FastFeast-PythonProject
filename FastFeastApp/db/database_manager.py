"""
DatabaseManager — Singleton Pattern.

One instance, one connection.
Logging via Audit instance — writes to db/logs/pipeline.log daily.
"""
from __future__ import annotations
from contextlib import contextmanager
import os
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
from helpers.logger_builder import build_file_logger


_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
_logger  = build_file_logger("fastfeast.db", _LOG_DIR)


class DatabaseManager:

    _instance: DatabaseManager | None = None

    def __new__(cls, audit=None) -> DatabaseManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._audit      = audit
            cls._instance._connection = cls._instance._create_connection()
        return cls._instance

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

    # ── Connection ────────────────────────────────────────────────────────

    def _create_connection(self) -> SnowflakeConnection:
        try:
            conn = snowflake.connector.connect(
                account                   = SNOWFLAKE_ACCOUNT,
                user                      = SNOWFLAKE_USER,
                password                  = SNOWFLAKE_PASSWORD,
                database                  = SNOWFLAKE_DATABASE,
                schema                    = SNOWFLAKE_SCHEMA,
                warehouse                 = SNOWFLAKE_WAREHOUSE,
                role                      = SNOWFLAKE_ROLE,
                client_session_keep_alive = True
            )
            self._log_success(
                f"[DatabaseManager._create_connection] "
                f"Connection established | "
                f"account={SNOWFLAKE_ACCOUNT} | "
                f"database={SNOWFLAKE_DATABASE} | "
                f"warehouse={SNOWFLAKE_WAREHOUSE}"
            )
            return conn
        except Exception as e:
            self._log_failure(
                f"[DatabaseManager._create_connection] "
                f"Failed to establish Snowflake connection | "
                f"Reason: Could not authenticate or reach the Snowflake account | "
                f"Raw error: {e}"
            )
            raise

    def _is_alive(self) -> bool:
        try:
            self._connection.cursor().execute("SELECT 1")
            return True
        except Exception as e:
            self._log_warning(
                f"[DatabaseManager._is_alive] "
                f"Connection ping failed — session may have expired | "
                f"Raw error: {e}"
            )
            return False

    def _ensure_connection(self) -> None:
        if self._connection.is_closed() or not self._is_alive():
            self._log_warning(
                f"[DatabaseManager._ensure_connection] "
                f"Connection is closed or unresponsive — attempting reconnect"
            )
            try:
                self._connection = self._create_connection()
            except Exception as e:
                self._log_failure(
                    f"[DatabaseManager._ensure_connection] "
                    f"Reconnection failed | "
                    f"Reason: Unable to re-establish Snowflake session | "
                    f"Raw error: {e}"
                )
                raise

    @property
    def connection(self) -> SnowflakeConnection:
        return self._connection

    # ── Execution ─────────────────────────────────────────────────────────

    @contextmanager
    def cursor_scope(self) -> SnowflakeCursor:
        self._ensure_connection()
        cursor = self._connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(self, sql: str, params: tuple = None) -> list:
        with self.cursor_scope() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def close(self) -> None:
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._log_success(
                f"[DatabaseManager.close] Connection closed cleanly"
            )