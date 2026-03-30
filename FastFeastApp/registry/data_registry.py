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
 
import importlib
import re
from typing import Optional, TYPE_CHECKING

import pandas as pd
import yaml

if TYPE_CHECKING:
    from conf_file_parser import ConfFileParser
 
 
def _camel_to_snake(name: str) -> str:
    """
    Convert a CamelCase class name to snake_case module filename.
 
    Examples
    --------
    "Agent"          → "agent"
    "TicketEvent"    → "ticket_event"
    "ReasonCategory" → "reason_category"
    """
    snake = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name)
    return snake.lower()
 
 
def _resolve_model_class(class_name: str) -> Optional[type]:
    """
    Dynamically import and return a model class by its CamelCase name.
 
    Convention:  class_name → models.<snake_case_name>.<class_name>
 
    Examples
    --------
    "Agent"       → from models.agent        import Agent
    "TicketEvent" → from models.ticket_event import TicketEvent
    """
    module_name = _camel_to_snake(class_name)
    module_path = f"models.{module_name}"
    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ModuleNotFoundError, AttributeError) as e:
        print(f"[DataRegistry] WARNING: could not resolve model '{class_name}' "
              f"from '{module_path}': {e}")
        return None
 
 
class DataRegistry:
 
    def __init__(self, parser: "ConfFileParser") -> None:
        self._parser = parser
 
        # ── raw configuration (loaded later) ──────────────────────────────
        self._conf: dict = {}
        self._files_conf: dict = {}
        self._tables_conf: dict = {}

        # ── in-memory DataFrame store ──────────────────────────────────
        self._df_store: dict[str, pd.DataFrame] = {}
 
        # ── repositories: table_key → repo instance ────────────────────
        # Populated manually via register_repository()
        self._repositories: dict[str, object] = {}
 
        # ── models: file_key → model class ────────────────────────────
        # Populated automatically in _build_maps()
        self._models: dict[str, type] = {}
 
        # ── lookup maps (all built from yaml) ─────────────────────────
        self._file_key_to_name:   dict[str, str]        = {}  # "agents"    → "agents.csv"
        self._file_name_to_key:   dict[str, str]        = {}  # "agents.csv"→ "agents"
        self._file_key_to_table:  dict[str, str]        = {}  # "agents"    → "AgentsDim"
        self._table_to_file_keys: dict[str, list[str]]  = {}  # "AgentsDim" → ["agents","teams"]
 
    def load_config(self, conf_path: str) -> None:
        with open(conf_path, "r") as f:
            self._conf = yaml.safe_load(f) or {}

        # Extract sections safely (no assumptions)
        self._files_conf = self._conf.get("files", {})
        self._tables_conf = self._conf.get("tables", {})

        # Reset maps before rebuilding
        self._file_key_to_name.clear()
        self._file_name_to_key.clear()
        self._file_key_to_table.clear()
        self._table_to_file_keys.clear()
        self._models.clear()

        # Only build maps if sections exist
        if self._files_conf:
            self._build_file_maps()

        if self._tables_conf:
            self._build_table_maps()
            
    # ══════════════════════════════════════════════════════════════════
    # Internal: build all maps from yaml — called once in __init__
    # ══════════════════════════════════════════════════════════════════
    def _build_maps(self) -> None:
        self._build_file_maps()
        self._build_table_maps()
 
    def _build_file_maps(self) -> None:
        """
        Walk yaml → files section.
        Builds:
          file_key ↔ file_name
          file_key  → Model class  (auto-resolved via importlib)
        """ 
        for file_key, file_conf in self._files_conf.items():
 
            # file_key ↔ file_name
            file_name = self._parser.get_file_name(file_conf)
            self._file_key_to_name[file_key] = file_name
            if file_name:
                self._file_name_to_key[file_name] = file_key
 
            # file_key → Model class
            class_name: str = file_conf.get("model_class", "")
            if class_name:
                model_cls = _resolve_model_class(class_name)
                if model_cls is not None:
                    self._models[file_key] = model_cls
 
    def _build_table_maps(self) -> None:
        """
        Walk yaml → tables section.
        Builds:
          table_key → [file_keys]
          file_key  → table_key   (first table wins if a file feeds multiple)
        """ 
        for table_key, table_conf in self._tables_conf.items():
            sources: list = self._parser.get_target_source(table_conf) or []
            self._table_to_file_keys[table_key] = sources
 
            for file_key in sources:
                # Many source files → one target table (e.g. agents+teams → AgentsDim)
                # First assignment wins to avoid accidental overwrite
                if file_key not in self._file_key_to_table:
                    self._file_key_to_table[file_key] = table_key
 
    # ══════════════════════════════════════════════════════════════════
    # Bootstrap: register repositories  (called once at pipeline startup)
    # ══════════════════════════════════════════════════════════════════
    def register_repository(self, table_key: str, repo_instance: object) -> None:
        """
        Attach a repository instance to a DWH table key.
 
        Parameters
        ----------
        table_key     : matches yaml tables key   e.g. "AgentsDim"
        repo_instance : already-constructed repo  e.g. AgentRepository(session)
 
        Example
        -------
        registry.register_repository("AgentsDim",    AgentRepository(session))
        registry.register_repository("CustomersDim", CustomerRepository(session))
        registry.register_repository("FactTickets",  FactTicketsRepository(session))
        """
        self._repositories[table_key] = repo_instance
 
    # ══════════════════════════════════════════════════════════════════
    # Model lookups   file_key / file_name → Model class
    # ══════════════════════════════════════════════════════════════════
    def get_model(self, file_key: str) -> Optional[type]:
        """
        Return the Model class for a file_key.
 
        Example
        -------
        registry.get_model("agents")  # → <class 'models.agent.Agent'>
        registry.get_model("orders")  # → <class 'models.order.Order'>
        """
        return self._models.get(file_key)
 
    def get_model_by_file_name(self, file_name: str) -> Optional[type]:
        """
        Return the Model class for a raw filename.
 
        Example
        -------
        registry.get_model_by_file_name("agents.csv")         # → Agent
        registry.get_model_by_file_name("ticket_events.json") # → TicketEvent
        """
        file_key = self._file_name_to_key.get(file_name)
        return self._models.get(file_key) if file_key else None
 
    # ══════════════════════════════════════════════════════════════════
    # Repository lookups   table_key / file_key / file_name → repo
    # ══════════════════════════════════════════════════════════════════
    def get_repository(self, table_key: str) -> Optional[object]:
        """
        Return the repository for a DWH table key.
 
        Example
        -------
        registry.get_repository("AgentsDim")   # → AgentRepository instance
        registry.get_repository("FactTickets") # → FactTicketsRepository instance
        """
        return self._repositories.get(table_key)
 
    def get_repository_for_file(self, file_key: str) -> Optional[object]:
        """
        Return the repository of the DWH table this file_key feeds into.
        agents + teams both feed AgentsDim → same repo returned for both.
 
        Example
        -------
        registry.get_repository_for_file("agents")  # → AgentRepository
        registry.get_repository_for_file("teams")   # → AgentRepository  (same!)
        registry.get_repository_for_file("tickets") # → FactTicketsRepository
        registry.get_repository_for_file("orders")  # → FactTicketsRepository
        """
        table_key = self._file_key_to_table.get(file_key)
        return self._repositories.get(table_key) if table_key else None
 
    def get_repository_for_file_name(self, file_name: str) -> Optional[object]:
        """
        Same as get_repository_for_file but accepts the actual filename.
 
        Example
        -------
        registry.get_repository_for_file_name("agents.csv")  # → AgentRepository
        registry.get_repository_for_file_name("orders.json") # → FactTicketsRepository
        """
        file_key = self._file_name_to_key.get(file_name)
        return self.get_repository_for_file(file_key) if file_key else None
 
    # ══════════════════════════════════════════════════════════════════
    # Name / key resolution
    # ══════════════════════════════════════════════════════════════════
    def get_file_name(self, file_key: str) -> Optional[str]:
        """  "agents" → "agents.csv"  |  "cities" → "cities.json"  """
        return self._file_key_to_name.get(file_key)
 
    def get_file_key(self, file_name: str) -> Optional[str]:
        """  "agents.csv" → "agents"  """
        return self._file_name_to_key.get(file_name)
 
    def get_table_key_for_file(self, file_key: str) -> Optional[str]:
        """
        "agents"   → "AgentsDim"
        "segments" → "CustomersDim"
        "tickets"  → "FactTickets"
        "orders"   → "FactTickets"
        """
        return self._file_key_to_table.get(file_key)
 
    def get_table_key_for_file_name(self, file_name: str) -> Optional[str]:
        """
        "agents.csv"  → "AgentsDim"
        "orders.json" → "FactTickets"
        """
        file_key = self._file_name_to_key.get(file_name)
        return self._file_key_to_table.get(file_key) if file_key else None
 
    def get_file_keys_for_table(self, table_key: str) -> list[str]:
        """
        "AgentsDim"    → ["agents", "teams"]
        "CustomersDim" → ["customers", "segments"]
        "FactTickets"  → ["tickets"]
        """
        return self._table_to_file_keys.get(table_key, [])
 
    def get_all_table_keys(self) -> list[str]:
        return list(self._table_to_file_keys.keys())
 
    def get_all_file_keys(self) -> list[str]:
        return list(self._file_key_to_name.keys())
 
    # ══════════════════════════════════════════════════════════════════
    # Config pass-through  (parser is always the source of truth)
    # ══════════════════════════════════════════════════════════════════
    def get_table_conf(self, table_key: str) -> dict:
        return self._parser.get_table_conf(self._tables_conf, table_key)
 
    def get_file_conf(self, file_key: str) -> dict:
        return self._parser.get_file_conf(self._files_conf,file_key)
 
    # ══════════════════════════════════════════════════════════════════
    # DataFrame store  (shared between pipeline phases)
    # ══════════════════════════════════════════════════════════════════
    def store_df(self, key: str, df: pd.DataFrame) -> None:
        self._df_store[key] = df
 
    def get_df(self, key: str) -> Optional[pd.DataFrame]:
        return self._df_store.get(key)
 
    def drop_df(self, key: str) -> None:
        """Free memory after a phase no longer needs the DataFrame."""
        self._df_store.pop(key, None)
 
    # ══════════════════════════════════════════════════════════════════
    # Debug / introspection
    # ══════════════════════════════════════════════════════════════════
    def summary(self) -> None:
        """Print a human-readable snapshot of all maps and registrations."""
        print("=" * 62)
        print("  DataRegistry Summary")
        print("=" * 62)
 
        print("\n── file_key → file_name ──────────────────────────────────")
        for fk, fn in self._file_key_to_name.items():
            print(f"  {fk:<25} → {fn}")
 
        print("\n── file_key → Model class ────────────────────────────────")
        for fk, cls in self._models.items():
            print(f"  {fk:<25} → {cls.__name__}")
 
        print("\n── file_key → DWH table_key ──────────────────────────────")
        for fk, tk in self._file_key_to_table.items():
            print(f"  {fk:<25} → {tk}")
 
        print("\n── DWH table_key → file_keys ─────────────────────────────")
        for tk, fks in self._table_to_file_keys.items():
            print(f"  {tk:<25} → {fks}")
 
        print("\n── Registered repositories ───────────────────────────────")
        if self._repositories:
            for tk, repo in self._repositories.items():
                print(f"  {tk:<25} → {type(repo).__name__}")
        else:
            print("  (none yet — call register_repository() at bootstrap)")
 
        print("\n── DataFrames in store ───────────────────────────────────")
        if self._df_store:
            for key, df in self._df_store.items():
                print(f"  {key:<25} → {len(df)} rows")
        else:
            print("  (empty)")
 
        print("=" * 62)