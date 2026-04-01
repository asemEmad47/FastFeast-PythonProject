"""
DataFlowTasksCreator — Factory that builds a DataFlowTask for a given
DWH table based on batch_mode ("batch" or "micro_batch").

batch_mode = "batch"       → dimension tables (agents, customers, etc.)
batch_mode = "micro_batch" → fact tables      (FactTickets)

Called once per DWH table by WorkFlow.orchestrate().
"""
from __future__ import annotations
from registry.conf_file_parser import ConfFileParser
from registry.data_registry    import DataRegistry
from audit.audit               import Audit

from etl.data_flow_task                         import DataFlowTask
from etl.components.read_from_source_factory    import ReadFromSourceFactory
from etl.components.validator_component         import ValidatorComponent
from etl.components.pii_mask                    import PIIMask
from etl.components.quarantine_writer           import QuarantineWriter
from etl.components.transformer                 import Transformer
from etl.components.join                        import Join
from etl.components.load_to_target              import LoadToTarget
from etl.lookup.orphans_handler                 import OrphansHandler
from etl.lookup.duplicates_lookup               import DuplicatesLookUp


class DataFlowTasksCreator:

    def __init__(
        self,
        parser:   ConfFileParser,
        registry: DataRegistry,
        audit:    Audit,
        files:    list[str],
    ) -> None:
        self.parser   = parser
        self.registry = registry
        self.audit    = audit
        self.files    = files    # full file paths in current run

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC
    # ══════════════════════════════════════════════════════════════════

    def create_data_flow_task(
        self,
        batch_mode: str,
        table_key:  str,
        table_conf: dict,
        active_sources: list[str],
    ) -> DataFlowTask:
        """
        Build and return a DataFlowTask for the given table.

        batch_mode : "batch" or "micro_batch"
        table_key  : yaml tables key  e.g. "AgentsDim", "FactTickets"
        table_conf : full config block from pipeline.yaml
        active_sources: file_keys whose files are present in this run
        """

        # ── Build data_framse_dicts (one per source) ──────────────────
        data_framse_dicts = []
        for file_key in active_sources:
            data_framse_dicts.append({
                "dataframe": None,
                "dimension": table_key,
                "source":    file_key,
            })

        # ── Stage 1: before_join_components (one chain per source) ────
        before_join_components = []
        for file_key in active_sources:
            chain = self._build_before_join_chain(file_key, table_conf)
            before_join_components.append(chain)

        # ── Stage 2: join_task (only when multiple sources) ───────────
        join_task = None
        if len(active_sources) > 1:
            join_task = self._build_join(table_conf, active_sources)

        # ── Stage 3: after_join_components ────────────────────────────
        after_join_components = self._build_after_join_components(
            batch_mode, table_key, table_conf
        )

        return DataFlowTask(
            audit                  = self.audit,
            registry               = self.registry,
            data_framse_dicts      = data_framse_dicts,
            before_join_components = before_join_components,
            join_task              = join_task,
            after_join_components  = after_join_components,
        )

    # ══════════════════════════════════════════════════════════════════
    # STAGE BUILDERS
    # ══════════════════════════════════════════════════════════════════

    def _build_before_join_chain(
        self, file_key: str, table_conf: dict
    ) -> list:
        """Read → Validate → PIIMask → QuarantineWriter per source file."""
        file_name = self.registry.get_file_name(file_key)
        file_path = self._resolve_path(file_name)
        file_conf = self.parser.get_file_conf(file_key)

        chain = []

        # Read
        chain.append(
            ReadFromSourceFactory.create_source(file_path, self.registry, self.parser)
        )

        # Validate
        chain.append(
            ValidatorComponent(
                table_conf = file_conf,
                audit      = self.audit,
                registry   = self.registry,
            )
        )

        # PIIMask — if file has pii_fields
        pii_fields = file_conf.get("pii_fields", [])
        if pii_fields:
            chain.append(PIIMask(pii_fields=pii_fields, audit=self.audit))

        # QuarantineWriter — flush bad_rows to disk
        chain.append(QuarantineWriter(audit=self.audit))

        return chain

    def _build_join(
        self, table_conf: dict, active_sources: list[str]
    ) -> Join:
        """Build Join from pipeline.yaml joins section."""
        join_defs    = self.parser.get_join_config(table_conf) or []
        join_configs = []

        for jd in join_defs:
            left_ref  = jd.get("left", "")
            right_ref = jd.get("right", "")
            join_configs.append({
                "left_on":   left_ref.split(".")[-1],    # "tickets.order_id" → "order_id"
                "right_key": right_ref.split(".")[0],    # "orders.order_id"  → "orders"
                "right_on":  right_ref.split(".")[-1],   # "orders.order_id"  → "order_id"
                "type":      jd.get("type", "left"),
            })

        return Join(
            join_configs = join_configs,
            audit        = self.audit,
            registry     = self.registry,
        )

    def _build_after_join_components(
        self, batch_mode: str, table_key: str, table_conf: dict
    ) -> list:
        """
        Transformer → OrphansHandler → DuplicatesLookUp
        → SCDComponent (batch only) → LoadToTarget.
        """
        components  = []
        primary_key = self.parser.get_target_primary_key(table_conf)

        # Transform
        components.append(Transformer(table_conf=table_conf, audit=self.audit))

        # Orphan check
        foreign_keys = self.parser.get_target_foreign_keys(table_conf)
        if foreign_keys:
            components.append(
                OrphansHandler(
                    foreign_keys = foreign_keys,
                    registry     = self.registry,
                    audit        = self.audit,
                )
            )

        # Dedup
        components.append(
            DuplicatesLookUp(
                primary_key = primary_key,
                table_key   = table_key,
                registry    = self.registry,
                audit       = self.audit,
            )
        )

        # SCD — only for batch (dimension) mode
        if batch_mode == "batch":
            scd_conf = table_conf.get("dimension", {})
            components.append(
                SCDComponent(
                    table_conf = table_conf,
                    scd_conf   = scd_conf,
                    registry   = self.registry,
                    audit      = self.audit,
                )
            )

        # Load — always last
        repo = self.registry.get_repository(table_key)
        components.append(
            LoadToTarget(
                source    = table_key,
                repo      = repo,
                registry  = self.registry,
                audit     = self.audit,
                pk_column = primary_key,
            )
        )

        return components

    # ══════════════════════════════════════════════════════════════════
    # HELPER
    # ══════════════════════════════════════════════════════════════════

    def _resolve_path(self, file_name: str) -> str:
        for f in self.files:
            if f.endswith(file_name):
                return f
        return file_name
