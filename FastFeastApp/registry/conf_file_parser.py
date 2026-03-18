"""
ConfFileParser — Reads and exposes pipeline.yaml configuration.

Key design: ConfFileParser is the ONLY class that reads the yaml file.
Every other class calls parser.get_*() — never reads the file directly.
"""
from __future__ import annotations
import yaml
from typing import Optional


class ConfFileParser:

    def __init__(self) -> None:
        self._conf: dict = {}

    # ------------------------------------------------------------------
    def parse(self, file_path: str) -> dict:
        """Load yaml file. Returns the full conf dict."""
        with open(file_path, "r") as f:
            self._conf = yaml.safe_load(f)
        return self._conf

    # ------------------------------------------------------------------
    def get_all_tables_conf(self) -> dict:
        """Return the full tables section."""
        return self._conf.get("tables", {})

    def get_table_conf(self, table_key: str) -> dict:
        """Return config for a table_key (e.g. 'customers')."""
        return self._conf.get("tables", {}).get(table_key, {})

    def get_table_conf_by_file(self, file_name: str) -> dict:
        """Find a table block by its file_name field."""
        for key, conf in self._conf.get("tables", {}).items():
            if conf.get("file_name") == file_name:
                return conf
        return {}

    def get_workflow_files(self) -> list[str]:
        return self._conf.get("workflow", {}).get("files", [])

    def get_target_table_name(self, table_key: str) -> str:
        return self.get_table_conf(table_key).get("target_table", "")

    def get_target_foreign_keys(self, table_key: str) -> dict:
        return self.get_table_conf(table_key).get("foreign_keys", {})

    def get_target_required_fields(self, table_key: str) -> list[str]:
        return self.get_table_conf(table_key).get("required_fields", [])

    def get_join_config(self, table_key: str) -> list[dict]:
        return self.get_table_conf(table_key).get("dimension_sources", [])

    def get_scd_type(self, table_key: str) -> int:
        dim = self.get_table_conf(table_key).get("dimension", {})
        return dim.get("scd_type", 1)

    def get_scd_tracked_columns(self, table_key: str) -> list[str]:
        return self.get_table_conf(table_key).get("tracked_columns", [])
