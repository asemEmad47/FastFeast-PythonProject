"""
DataFlowTasksCreator — Factory that builds a DataFlowTask for a given
DWH table based on batch_mode ("batch" or "micro_batch").

batch_mode = "batch"       → dimension tables (agents, customers, etc.)
batch_mode = "micro_batch" → fact tables      (FactTickets)

Called once per DWH table by WorkFlow.orchestrate().
"""
from __future__ import annotations
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
from etl.lookup.orphan_lookup                import OrphanLookUp

#from etl.scd.scd_component                     import SCDComponent

##### To-do list:
##### 1- add file paths to dictionary
##### 2- Replace parser with registry to use functions from registry class & remove the parser
##### 3- Foriegn keys

class DataFlowTasksCreator:

    def __init__(self, parser, registry, audit, sources, files):
        self.parser = parser
        self.registry = registry
        self.audit = audit
        self.sources = sources
        self.files = files

    # ═══════════════════════════════════════════════════════════════
    # PUBLIC METHOD
    # ═══════════════════════════════════════════════════════════════
    def create_data_flow_task(
        self,
        batch_mode: str,
        table_key: str,
        table_conf: dict,
        active_sources: list[str],
        files: list[str]
    ) -> DataFlowTask:

        # ── Stage 1: dataframe_dicts ─────────────────────────────
        data_framse_dicts = [
            {
                "dataframe": None,
                "dimension": table_key,
                "source": file_key,
            }
            for file_key in active_sources
        ]

        # ── Stage 1: before_join_components (DICT) ──────────────
        before_join_components = {}

        for file_key in active_sources:
            before_join_components[file_key] = self._build_before_join_chain(file_key)

        # ── Stage 2: join ───────────────────────────────────────
        join_task = None
        if len(active_sources) > 1:
            #join_task = self._build_join(table_conf)
            join_task = self._build_join(table_key)

        # ── Stage 3: after join ─────────────────────────────────
        after_join_components = self._build_after_join_components(
            batch_mode, table_key, table_conf
        )

        task = DataFlowTask(
            audit=self.audit,
            registry=self.registry,
            before_join_components=before_join_components,
            join_task=join_task,
            after_join_components=after_join_components,
        )

        # 🔥 IMPORTANT
        task.dataframe_dicts = data_framse_dicts

        return task

    # ══════════════════════════════════════════════════════════════════
    # STAGE BUILDERS
    # ══════════════════════════════════════════════════════════════════

    def _build_before_join_chain(self, file_key: str) -> list:

        # ✔ correct usage
        file_conf = self.registry.get_file_conf(file_key)
        file_name = self.registry.get_file_name(file_key)

        file_path = self._resolve_path(file_name)
        

        chain = []
        

        chain.append(
            ReadFromSourceFactory.create_source(
                file_name=file_path,
            )
        )

        # Validate
        chain.append(
            ValidatorComponent(
                audit=self.audit,
                registry=self.registry,
            )
        )

        # PII
        pii_fields = self.registry.get_pii_columns(file_key)
        if pii_fields:
            chain.append(PIIMask(self.audit,self.registry))

        # Quarantine
        chain.append(QuarantineWriter(audit=self.audit))

        return chain
    

    def _build_join(self, table_key: str) -> Join:

        join_configs = self.registry.get_join_config(table_key)

        return Join(
            audit=self.audit,
            registry=self.registry,
            )
    


    def _build_after_join_components(self, batch_mode, table_key, table_conf):

        components = []

        primary_key = self.registry.get_target_primary_key(table_key)

        # 1. Transformer
        components.append(
            Transformer( self.audit , self.registry)
        )

        # 2. OrphansHandler (always for both modes)
        foreign_keys = self.registry.get_target_foreign_keys(table_key)

        if foreign_keys:
            components.append(
                OrphansHandler(
                    foreign_keys=foreign_keys,
                    registry=self.registry,
                    audit=self.audit,
                )
            )

        # 3. Deduplicates
        components.append(
            DuplicatesLookUp(
                primary_key=primary_key,
                table_key=table_key,
                registry=self.registry,
                audit=self.audit,
            )
        )

        # 4. Load
        repo = self.registry.get_repository(table_key)

        components.append(
            LoadToTarget(
                source=table_key,
                repo=repo,
                registry=self.registry,
                audit=self.audit,
                pk_column=primary_key,
            )
        )

        # 5. 🔥 OrphanLookUp ONLY for microbatch
        if batch_mode in ["micro_batch", "microbatch"] and foreign_keys:

            for fk_column, fk_conf in foreign_keys.items():

                dim_table = fk_conf.get("dim_table")
                pk_column = fk_conf.get("pk_column")

                components.append(
                    OrphanLookUp(
                        fk_column=fk_column,
                        dim_table=dim_table,
                        pk_column=pk_column,
                        registry=self.registry,
                        audit=self.audit,
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
