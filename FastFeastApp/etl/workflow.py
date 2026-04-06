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
from etl.tasks.email_task               import EmailTask
from registry.data_registry             import DataRegistry
from utils.file_tracker                 import FileTracker
from validation.validator_context       import ValidatorContext
from etl.data_flow_tasks_creator        import DataFlowTasksCreator
from etl.data_flow_task import DataFlowTask
from etl.task import Task
#to do list
# pass path of files which we will process them only
class WorkFlow(Task):

    batch_mode: str
    validator: ValidatorContext
    registry: DataRegistry
    audit: Audit
    data_flow_task: DataFlowTask
    alerter: EmailTask
    

    def __init__(
        self,
        batch_mode: str,
        registry: DataRegistry,
        audit: Audit,
        alerter: EmailTask,
        validator: ValidatorContext,
        

    ) -> None:
        self.batch_mode = batch_mode
        self.registry = registry
        self.audit = audit
        self.alerter = alerter
        self.validator = validator
        self.data_flow_task = None
        
    def orchestrate(self, files: list[str]) -> None:

        all_tables_conf = self.registry.get_all_tables_conf()

        matched_data = []

        for table_key, table_conf in all_tables_conf.items():

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

            matched_files = self._get_files_for_sources(active_sources, files)

            matched_data.append({
                "table": table_key,
                "sources": active_sources,
                "files": matched_files
            })

        print("Matched Data:", matched_data)
        creator = DataFlowTasksCreator(
            registry=self.registry,
            audit=self.audit,
            data=matched_data
        )

        self.data_flow_task, dataframe_dicts = creator.create_data_flow_task(
            batch_mode=self.batch_mode,
            matched_data=matched_data
        )

        ok, errors = self.data_flow_task.do_task(dataframe_dicts)

        if not ok:
            self.trigger_alert(errors)

    # ────────────── Alert ──────────────
    def trigger_alert(self, message: str) -> None:
        pass

    # ────────────── Helper ──────────────
    def _file_in_run(self, file_key: str, files: list[str]) -> bool:
        file_name = self.registry.get_file_name(file_key)
        if not file_name:
            return False
        return any(f.endswith(file_name) for f in files)

    def _get_files_for_sources(self, sources, files):
        result = []
        for s in sources:
            file_name = self.registry.get_file_name(s)
            for f in files:
                if f.endswith(file_name):
                    result.append(f)
        return result

    def _is_static_and_processed(self, file_key: str) -> bool:
        file_type = self.registry.get_file_type(file_key)

        if file_type == "static":
            return True

        return False
    
    def do_task(self):
        pass