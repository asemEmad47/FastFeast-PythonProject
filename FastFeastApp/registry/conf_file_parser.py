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
        self._batch_conf: dict = {}

    # ------------------------------------------------------------------
    #  YAML parsing
    # ------------------------------------------------------------------
    def parse(self, file_path: str) -> dict:
        with open(file_path, "r") as f:
            self._conf = yaml.safe_load(f)
        return self._conf

    def parse_batch_conf(self, batch_file_path: str) -> dict:
        with open(batch_file_path, "r") as f:
            self._batch_conf = yaml.safe_load(f)
        return self._batch_conf

    # ------------------------------------------------------------------
    # Table config getters
    # ------------------------------------------------------------------
    def get_all_tables_conf(self) -> dict:
        """Return the full tables section."""
        return self._conf.get("tables", {})
    
    def get_table_conf(self, table_key: str) -> dict:
        return self._conf.get("tables", {}).get(table_key, {})

    def get_target_table_name(self, conf_section: dict) -> str:
        return conf_section.get("target_table", "")
    
    def get_target_table_type(self, conf_section: dict) -> str:
        return conf_section.get("table_type", "")

    def get_target_primary_key(self, conf_section: dict) -> dict:
        return conf_section.get("primary_key", {})
    
    def get_target_foreign_keys(self, conf_section: dict) -> dict:
        return conf_section.get("foreign_keys", {})

    def get_target_required_fields(self, conf_section: dict) -> list[str]:
        return conf_section.get("required_fields", [])
    
    def get_target_source(self, conf_section: dict) -> list[str]:
        return conf_section.get("source")

    def get_target_columns(self, conf_section: dict) -> list[str]:
        return conf_section.get("keep_columns")
    
    def get_join_config(self, conf_section: dict) -> list[dict]:
        return conf_section.get("joins", [])
    
    def get_fact_join_config(self, conf_section: dict) -> list[dict]:
        """Return list of dimension_sources / join info."""
        return conf_section.get("dimension_sources", [])
    
    def get_fact_aggregated_columns(self, conf_section: dict) -> list[dict]:
        return conf_section.get("aggregated_columns", [])

    # ------------------------------------------------------------------
    # File config getters
    # ------------------------------------------------------------------

    def get_workflow_files(self) -> list[str]:
        return self._conf.get("files", [])
    
    def get_file_conf(self, file_key: str) -> dict:
        return self._conf.get("files", {}).get(file_key, {})

    def get_file_required_fields(self, conf_section: dict) -> list[str]:
        return conf_section.get("required_fields", [])
    
    def get_file_type(self, conf_section: dict) -> list[str]:
        return conf_section.get("file_type", [])
    
    def get_file_name(self, conf_section: dict) -> list[str]:
        return conf_section.get("file_name", [])
    
    def get_file_columns(self, conf_section: dict) -> list[str]:
        return conf_section.get("file_columns", [])


    # ------------------------------------------------------------------
    # Batch / microbatch / archive paths
    # ------------------------------------------------------------------

    def get_batch_interval(self) -> str:
        return self._batch_conf.get("batch", {}).get("batch_interval")

    def get_batch_path(self) -> str:
        return self._batch_conf.get("batch", {}).get("batch_path")

    def get_microbatch_path(self) -> str:
        return self._batch_conf.get("batch", {}).get("micro_batch_path")

    def get_archive_dir(self) -> str:
        return self._batch_conf.get("batch", {}).get("archive_dir_path")
        