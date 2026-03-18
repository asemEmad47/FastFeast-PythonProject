"""
WorkFlow — Orchestration Facade.

Four phases per run:
  Phase 1  do_non_join_tasks()  per-file:  Read → Validate → PIIMask → Quarantine
  Phase 2  do_join_task()       fact enrichment: join dims into facts
  Phase 3  _do_after_action()   post-join: Transformer → OrphansHandler → DuplicatesLookUp → SCD
  Phase 4  FactLoadTask         dims first, then facts

trigger_alert() always fires on a daemon thread — never blocks the pipeline.
"""
import threading
from typing import Optional
import pandas as pd

from audit.audit import Audit
from audit.email_task import EmailTask
from registry.conf_file_parser import ConfFileParser
from etl.data_flow_task import DataFlowTask
from etl.fact_load_task import FactLoadTask
from etl.components.read_from_source_factory import ReadFromSourceFactory
from etl.components.validator_component import ValidatorComponent
from etl.components.pii_mask import PIIMask
from etl.components.quarantine_writer import QuarantineWriter
from etl.components.transformer import Transformer
from etl.components.load_to_target import LoadToTarget
from etl.components.join import Join
from etl.lookup.orphans_handler import OrphansHandler
from etl.lookup.duplicates_lookup import DuplicatesLookUp
from etl.scd.scd_component import SCDComponent
from registry.data_registry import DataRegistry
from utils.file_tracker import FileTracker


class WorkFlow:

    def __init__(self) -> None:
        self.files:        list[str]      = []
        self.registry:     DataRegistry   = None
        self.parser:       ConfFileParser = None
        self.audit:        Audit          = None
        self.alerter:      EmailTask      = None
        self.file_tracker: FileTracker    = None

        # Single shared instances — reused per file
        self._before_join_task: DataFlowTask = None
        self._after_join_task:  DataFlowTask = None
        self._join_task:        Join         = None
        self._load_to_fact:     FactLoadTask = None

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC
    # ══════════════════════════════════════════════════════════════════

    def orchestrate(self) -> None:
        self.audit.reset()
        self.audit.start_timer()

        self.do_non_join_tasks()   # Phase 1
        self.do_join_task()        # Phase 2
        self._do_after_action()    # Phase 3

        # Phase 4
        load_task = self._build_fact_load_task()
        ok, errors = load_task.do_task()
        if not ok:
            self._trigger_alert(f"FactLoadTask failed: {errors}")
            return

        # Mark processed + archive
        for f in self.files:
            self.file_tracker.mark_processed(f)
        self.file_tracker.move_files_to_archive(self.files)

        # Persist audit row to DWH
        run_log_repo = self.registry.get_repository_for_file("run_log")
        self.audit.persist_to_dwh(run_log_repo)
        self.audit.log_success("Pipeline run completed")

    # ══════════════════════════════════════════════════════════════════
    # PHASE 1 — per-file pre-join
    # ══════════════════════════════════════════════════════════════════

    def do_non_join_tasks(self) -> None:
        """
        For each file: Read → ValidatorComponent → PIIMask → QuarantineWriter
        Results stored in DataRegistry by file name.
        """
        for file in self.files:
            task = self._build_before_join_task(file)
            ok, errors, df = task.do_task()

            self.audit.track_metrics(file, {
                "total_records": len(df) if df is not None else 0,
                "errors": errors,
            })

            if not ok:
                self._trigger_alert(f"Pre-join task failed: {file} — {errors}")
                continue

            self.registry.store_df(file, df)

    # ══════════════════════════════════════════════════════════════════
    # PHASE 2 — join dims into facts
    # ══════════════════════════════════════════════════════════════════

    def do_join_task(self) -> None:
        """
        For every FACT in config whose source file is in self.files:
          Iterate dimension_sources → fetch right_df from registry → join.
          Store enriched fact df under output_alias.
        """
        all_tables = self.parser.get_all_tables_conf()

        for table_key, table_conf in all_tables.items():
            if table_conf.get("table_type") != "fact":
                continue
            file_name = table_conf.get("file_name")
            if file_name not in self.files:
                continue

            fact_df = self.registry.get_df(file_name)
            if fact_df is None:
                continue

            for dim_source in table_conf.get("dimension_sources", []):
                dim_key   = dim_source["dim_key"]
                fact_fk   = dim_source["fact_fk"]
                join_type = dim_source.get("join_type", "left")

                dim_conf = self.parser.get_table_conf(dim_key)
                right_df = self.registry.get_df(dim_conf["file_name"])

                if right_df is None:
                    self._trigger_alert(f"Join skipped — dim df missing: {dim_key}")
                    continue

                join = Join(
                    left_on   = fact_fk,
                    right_on  = dim_conf["primary_key"],
                    join_type = join_type,
                    audit     = self.audit,
                )
                ok, errors, fact_df = join.do_task(fact_df, right_df)

                if not ok:
                    self._trigger_alert(f"Join failed: {file_name} ← {dim_key}: {errors}")
                    break

            self.audit.track_metrics("join", {"output_rows": len(fact_df)})
            alias = table_conf.get("output_alias", file_name)
            self.registry.store_df(alias, fact_df)

    # ══════════════════════════════════════════════════════════════════
    # PHASE 3 — post-join: transform + orphan + dedup + SCD
    # ══════════════════════════════════════════════════════════════════

    def _do_after_action(self) -> None:
        """
        For each table in the run (dims use file_name, facts use output_alias):
          Inject df from registry → run Transformer → OrphansHandler
          → DuplicatesLookUp → SCDComponent.
        """
        all_tables = self.parser.get_all_tables_conf()

        for table_key, table_conf in all_tables.items():
            file_name = table_conf.get("file_name")
            if file_name not in self.files:
                continue

            source_key = table_conf.get("output_alias", file_name)
            df = self.registry.get_df(source_key)
            if df is None:
                continue

            task = self._build_after_join_task(table_key, table_conf)
            task.set_input_df(df)
            ok, errors, result_df = task.do_task()

            self.audit.track_metrics(source_key, {
                "final_records": len(result_df) if result_df is not None else 0,
                "errors": errors,
            })

            if not ok:
                self._trigger_alert(f"Post-join task failed: {source_key} — {errors}")
                continue

            self.registry.store_df(source_key, result_df)

    # ══════════════════════════════════════════════════════════════════
    # ALERT
    # ══════════════════════════════════════════════════════════════════

    def _trigger_alert(self, message: str) -> None:
        """Log failure + send async email — never blocks the pipeline."""
        self.audit.log_failure(message)
        t = threading.Thread(
            target=self.alerter.do_task,
            args=(message,),
            daemon=True,
        )
        t.start()

    # ══════════════════════════════════════════════════════════════════
    # TASK BUILDERS
    # ══════════════════════════════════════════════════════════════════

    def _build_before_join_task(self, file: str) -> DataFlowTask:
        """Read → ValidatorComponent → PIIMask (if configured) → QuarantineWriter"""
        table_conf = self.parser.get_table_conf_by_file(file)
        components = []

        components.append(
            ReadFromSourceFactory.create_source(file, self.registry, self.parser)
        )
        components.append(
            ValidatorComponent(parser=self.parser, registry=self.registry,
                               audit=self.audit, table_conf=table_conf)
        )
        pii_fields = table_conf.get("pii_fields", [])
        if pii_fields:
            components.append(PIIMask(pii_fields=pii_fields, audit=self.audit))

        components.append(QuarantineWriter(audit=self.audit))

        return DataFlowTask(components=components, source=file, audit=self.audit)

    def _build_after_join_task(self, table_key: str, table_conf: dict) -> DataFlowTask:
        """Transformer → OrphansHandler → DuplicatesLookUp → SCDComponent"""
        components = []

        components.append(Transformer(table_conf=table_conf, audit=self.audit))

        if table_conf.get("foreign_keys"):
            components.append(
                OrphansHandler(foreign_keys=table_conf["foreign_keys"],
                               registry=self.registry, audit=self.audit)
            )

        components.append(
            DuplicatesLookUp(primary_key=table_conf["primary_key"],
                             registry=self.registry, audit=self.audit)
        )

        scd_conf = table_conf.get("dimension", {})
        components.append(
            SCDComponent(table_conf=table_conf, scd_conf=scd_conf,
                         registry=self.registry, audit=self.audit)
        )

        source = table_conf.get("output_alias", table_conf.get("file_name"))
        return DataFlowTask(components=components, source=source, audit=self.audit)

    def _build_fact_load_task(self) -> FactLoadTask:
        """Separate loaders into dim_components and fact_components."""
        all_tables = self.parser.get_all_tables_conf()
        dim_components  = []
        fact_components = []

        for table_key, table_conf in all_tables.items():
            if table_conf.get("file_name") not in self.files:
                continue
            source = table_conf.get("output_alias", table_conf.get("file_name"))
            repo   = self.registry.get_repository_for_file(table_conf["file_name"])
            loader = LoadToTarget(source=source, repo=repo,
                                  registry=self.registry, audit=self.audit)

            if table_conf.get("table_type") == "dimension":
                dim_components.append(loader)
            else:
                fact_components.append(loader)

        return FactLoadTask(dim_components=dim_components,
                            fact_components=fact_components,
                            registry=self.registry, audit=self.audit)
