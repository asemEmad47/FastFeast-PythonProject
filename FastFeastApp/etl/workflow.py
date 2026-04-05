"""
WorkFlow — Orchestration Facade.

UML attributes:
  - batch_mode     : string          ("batch" or "micro_batch")
  - validator      : ValidatorContext
  - registry       : DataRegistry
  - parser         : ConfFileParser
  - audit          : Audit
  - data_flow_task : DataFlowTask     (one task, built by DataFlowTasksCreator)
  - alerter        : EmailTask

UML methods:
  + trigger_alert(message) : void
  + orchestrate(list[filenames]) : void

Flow:
  For each DWH table whose sources are in the file list:
    1. DataFlowTasksCreator.create_data_flow_task(batch_mode) → DataFlowTask
    2. data_flow_task.do_task()
    3. trigger_alert() on failure (async daemon thread)
  Then archive files and persist audit row.
"""
from __future__ import annotations
import threading

from audit.audit                        import Audit
from etl.tasks.email_task                   import EmailTask
from registry.data_registry             import DataRegistry
from registry.conf_file_parser             import ConfFileParser
from FastFeastApp.utils.file_tracker                 import FileTracker
from FastFeastApp.validation.validator_context       import ValidatorContext
from FastFeastApp.etl.data_flow_tasks_creator        import DataFlowTasksCreator
from FastFeastApp.etl.data_flow_task import DataFlowTask
from task import Task

class WorkFlow():

    batch_mode: str
    validator: ValidatorContext
    registry: DataRegistry
    audit: Audit
    data_flow_task: "DataFlowTask"
    alerter: EmailTask
    parser: ConfFileParser

    def __init__(
        self,
        batch_mode: str,
        registry: DataRegistry,
        audit: Audit,
        alerter: EmailTask,
        validator: ValidatorContext,
        parser: ConfFileParser

    ) -> None:
        self.batch_mode = batch_mode
        self.registry = registry
        self.audit = audit
        self.alerter = alerter
        self.validator = validator
        self.data_flow_task = None
        self.parser = parser

    def orchestrate(self, files: list[str]) -> None:

        creator = DataFlowTasksCreator(
            parser=self.parser,
            registry=self.registry,
            audit=self.audit,
            files=files,
        )

        all_tables = self.registry.get_all_tables_conf()
        for table_key, table_conf in all_tables.items():
            if table_conf.get("generated"):
                continue

            sources = self.registry.get_target_source(table_key) or []
            active_sources = [
                s for s in sources
                if self._file_in_run(s, files)
                and not self._is_static_and_processed(s)
            ]
            if not active_sources:
                continue

            # print(active_sources)

            self.data_flow_task = creator.create_data_flow_task(
                batch_mode=self.batch_mode,
                table_key=table_key,
                table_conf=table_conf,
                active_sources=active_sources,
            )

            ok, errors = self.data_flow_task.do_task()

    # ────────────── Alert ──────────────
    def trigger_alert(self, message: str) -> None:
        pass

    # ────────────── Helper ──────────────
    def _file_in_run(self, file_key: str, files: list[str]) -> bool:
        file_name = self.registry.get_file_name(file_key)
        if not file_name:
            return False
        return any(f.endswith(file_name) for f in files)

    def _is_static_and_processed(self, file_key: str) -> bool:
        file_type = self.registry.get_file_type(file_key)

        if file_type == "static":
            return True

        return False