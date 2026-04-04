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
from registry.conf_file_parser          import ConfFileParser
from registry.data_registry             import DataRegistry
from utils.file_tracker                 import FileTracker
from validation.validator_context       import ValidatorContext
from etl.data_flow_tasks_creator        import DataFlowTasksCreator


class WorkFlow:

    def __init__(self) -> None:
        self.batch_mode:     str            = ""
        self.registry:       DataRegistry   = None
        self.parser:         ConfFileParser = None
        self.audit:          Audit          = None
        self.alerter:        EmailTask      = None
        self.file_tracker:   FileTracker    = None
        self.validator:      ValidatorContext = None

        # Single DataFlowTask — rebuilt per table inside orchestrate()
        self.data_flow_task = None

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC
    # ══════════════════════════════════════════════════════════════════

    def orchestrate(self, files: list[str]) -> None:
        """
        Entry point called by Batch or MicroBatch with the list of
        file paths to process in this run.
        """
        self.audit.reset()
        self.audit.start_timer()

        creator    = DataFlowTasksCreator(
            parser   = self.parser,
            registry = self.registry,
            audit    = self.audit,
            files    = files,
        )

        all_tables = self.parser.get_all_tables_conf()

        for table_key, table_conf in all_tables.items():

            # Skip generated tables (e.g. DateDim — no source files)
            if table_conf.get("generated"):
                continue

            sources: list[str] = self.parser.get_target_source(table_conf) or []

            # Only process tables whose source files are in this run
            active_sources = [
                s for s in sources
                if self._file_in_run(s, files)
            ]
            if not active_sources:
                continue

            # Build and run one DataFlowTask per DWH table
            self.data_flow_task = creator.create_data_flow_task(
                batch_mode     = self.batch_mode,
                table_key      = table_key,
                table_conf     = table_conf,
                active_sources = active_sources,
            )

            ok, errors = self.data_flow_task.do_task()

            if not ok:
                self.trigger_alert(
                    f"Pipeline failed [{table_key}]: {errors}"
                )

        # ── Post-run ───────────────────────────────────────────────────
        self.file_tracker.mark_processed(files)
        self.file_tracker.move_files_to_archive(files)

        run_log_repo = self.registry.get_repository("RunLogRepo")
        self.audit.persist_to_dwh(run_log_repo)
        self.audit.log_success("Pipeline run completed")

    # ══════════════════════════════════════════════════════════════════
    # ALERT
    # ══════════════════════════════════════════════════════════════════

    def trigger_alert(self, message: str) -> None:
        """
        Log failure immediately.
        Send email notification on a daemon thread — never blocks the pipeline.
        """
        self.audit.log_failure(message)
        t = threading.Thread(
            target = self.alerter.do_task,
            args   = (message,),
            daemon = True,
        )
        t.start()

    # ══════════════════════════════════════════════════════════════════
    # HELPER
    # ══════════════════════════════════════════════════════════════════

    def _file_in_run(self, file_key: str, files: list[str]) -> bool:
        """Check if the file for this file_key is present in the current run."""
        file_name = self.registry.get_file_name(file_key)
        if not file_name:
            return False
        return any(f.endswith(file_name) for f in files)
