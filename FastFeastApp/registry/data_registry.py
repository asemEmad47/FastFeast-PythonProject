"""
DataRegistry — Registry Pattern + LookUp cache.

Central hub that maps file names to:
  - repository instances
  - model classes
  - table config dicts

Also owns the in-memory df store (store_df / get_df) used by WorkFlow
to share DataFrames between pipeline phases without re-reading from disk.

Two-tier LookUp strategy:
  lookup_cached()    — for small/stable tables, warmed at startup
  get_existing_ids() — bulk IN query for large/growing tables
"""
from __future__ import annotations
from typing import Optional, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from registry.conf_file_parser import ConfFileParser


class DataRegistry:

    def __init__(self, parser: "ConfFileParser") -> None:
        self._parser        = parser
        self._df_store:     dict[str, pd.DataFrame] = {}
        self._cache:        dict[str, set]           = {}
        self._repositories: dict[str, object]        = {}
        self._models:       dict[str, type]          = {}

    # ── DataFrame store ───────────────────────────────────────────────
    def store_df(self, key: str, df: pd.DataFrame) -> None:
        self._df_store[key] = df

    def get_df(self, key: str) -> Optional[pd.DataFrame]:
        return self._df_store.get(key)

    # ── Repository lookup ─────────────────────────────────────────────
    def get_repository_for_file(self, file_name: str):
        """
        Return the repository instance for a given source file name.
        TODO: populate _repositories in __init__ by mapping from config.
        """
        return self._repositories.get(file_name)

    def get_model_for_file(self, file_name: str):
        """
        Return the SQLAlchemy/Pydantic model class for a given source file.
        TODO: populate _models in __init__ by mapping from config.
        """
        return self._models.get(file_name)

    # ── Config lookup ─────────────────────────────────────────────────
    def get_table_conf(self, table_key: str) -> dict:
        """Return config block for a table_key (e.g. 'customers')."""
        return self._parser.get_table_conf(table_key)

    def get_table_conf_by_file(self, file_name: str) -> dict:
        return self._parser.get_table_conf_by_file(file_name)

    # ── Two-tier LookUp ───────────────────────────────────────────────
    def warm_cache(self, table_name: str, repo) -> None:
        """Load all IDs from a small/stable table into the in-memory cache."""
        ids = repo.get_all_ids()
        self._cache[table_name] = set(ids)

    def refresh_cache(self, table_name: str, repo) -> None:
        """Re-warm cache at start of each batch day."""
        self.warm_cache(table_name, repo)

    def lookup_cached(self, table_name: str, ids: set) -> set:
        """Return which IDs exist — pure in-memory, zero DB calls."""
        cached = self._cache.get(table_name, set())
        return cached.intersection(ids)

    def get_existing_ids(self, table_name: str, id_column: str, ids: set) -> set:
        """
        ONE bulk IN query — returns set of IDs that exist in the DWH.
        Used for large/growing tables (customers, orders, tickets).
        TODO: implement via repo.get_existing_ids(id_column, ids).
        """
        repo = self._repositories.get(table_name)
        if repo is None:
            return set()
        # placeholder — implement in BaseRepository
        # return repo.get_existing_ids(id_column, ids)
        return set()
