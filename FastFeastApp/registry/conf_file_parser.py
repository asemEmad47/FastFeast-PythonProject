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
        pass
    # ------------------------------------------------------------------
    # Table config getters
    # ------------------------------------------------------------------
    def get_all_tables_conf(self, conf_section: dict) -> dict:
        if not conf_section:
            return {}
        return conf_section.get("tables", {})

    def get_table_conf(self, conf_section: dict, table_key: str) -> dict:
        if not conf_section:
            return {}
        return conf_section.get(table_key, {})

    def get_target_table_name(self, conf_section: dict) -> str:
        if not conf_section:
            return ""
        return conf_section.get("target_table", "")
    
    def get_target_table_type(self, conf_section: dict) -> str:
        if not conf_section:
            return ""
        return conf_section.get("table_type", "")

    def get_target_primary_key(self, conf_section: dict) -> dict:
        if not conf_section:
            return {}
        return conf_section.get("primary_key", {})
    
    def get_target_foreign_keys(self, conf_section: dict) -> dict:
        if not conf_section:
            return {}
        return conf_section.get("foreign_keys", {})

    def get_target_required_fields(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("required_fields", [])
    
    def get_target_source(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("source")

    def get_target_columns(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("keep_columns",[])
    
    def get_join_config(self, conf_section: dict) -> list[dict]:
        if not conf_section:
            return []
        return conf_section.get("joins", [])
    
    def get_fact_join_config(self, conf_section: dict) -> list[dict]:
        if not conf_section:
            return []
        return conf_section.get("dimension_sources", [])
    
    def get_fact_aggregated_columns(self, conf_section: dict) -> list[dict]:
        if not conf_section:
            return []
        return conf_section.get("aggregated_columns", [])

    # ------------------------------------------------------------------
    # File config getters
    # ------------------------------------------------------------------

    def get_workflow_files(self, conf_section: dict) -> dict:
        if not conf_section:
            return {}
        return conf_section.get("files", {})

    def get_file_conf(self, files_section: dict, file_key: str) -> dict:
        if not files_section:
            return {}
        return files_section.get(file_key, {})

    def get_file_name(self, conf_section: dict) -> str:
        if not conf_section:
            return ""
        return conf_section.get("file_name", "")

    def get_file_required_fields(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("required_fields", [])
    
    def get_file_type(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("file_type", [])
    
    def get_file_name(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("file_name", [])
    
    def get_file_columns(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("file_columns", [])
    
    def get_pii_columns(self, conf_section: dict) -> list[str]:
        if not conf_section:
            return []
        return conf_section.get("pii_columns", [])


    # ------------------------------------------------------------------
    # Batch / microbatch / archive paths
    # ------------------------------------------------------------------

    def get_batch_interval(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("batch_interval", "")


    def get_batch_path(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("batch_path", "")


    def get_microbatch_path(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("micro_batch_path", "")


    def get_archive_dir(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("archive_dir_path", "")


    def get_archive_dir_batch(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("archive_dir_batch", "")


    def get_archive_dir_stream(self, batch_section: dict) -> str:
        if not batch_section:
            return ""
        return batch_section.get("archive_dir_stream", "")
            