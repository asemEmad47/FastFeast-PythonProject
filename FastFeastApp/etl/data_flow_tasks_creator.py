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
from etl.lookup.orphan_lookup                    import OrphanLookUp

#from etl.scd.scd_component                     import SCDComponent

##### To-do list:
##### 1- add file paths to dictionary
##### 2- Replace parser with registry to use functions from registry class & remove the parser
##### 3- Foriegn keys

class DataFlowTasksCreator:

    def __init__(self,registry, audit, data):
        self.registry = registry
        self.audit = audit
        self.data = data

    # ═══════════════════════════════════════════════════════════════
    # PUBLIC METHOD
    # ═══════════════════════════════════════════════════════════════
    def create_data_flow_task(
        self,
        batch_mode: str,
        matched_data: list
    ) -> DataFlowTask:

        # ── Stage 1: dataframe_dicts ─────────────────────────────
        dataframe_dicts = []
        before_join_components = {}

        for item in matched_data:
            table_key = item["table"]
            sources = item["sources"]
            files = item["files"]

            for file_key in sources:
                file_name = self.registry.get_file_name(file_key)

                # 🔥 match correct file path
                file_path = next(
                    (f for f in files if f.endswith(file_name)),
                    None
                )

                dataframe_dicts.append({
                    "dataframe": None,
                    "dimension": table_key,
                    "source": file_key,
                    "file_path": file_path
                })

                # build before join per source
                before_join_components[file_key] = self._build_before_join_chain(file_key,file_path)

        print("DataFrame Dicts:", dataframe_dicts)

        # ── Stage 2: join ───────────────────────────────────────
        join_task = None

        if any(len(item["sources"]) > 1 for item in matched_data):
            join_task = Join(
                audit=self.audit,
                registry=self.registry,
            )

        # ── Stage 3: after join ─────────────────────────────────
        after_join_components = {}

        for item in matched_data:
            table_key = item["table"]
            table_conf = self.registry.get_table_conf(table_key)

            # build components for this table only
            components = self._build_after_join_components(
                batch_mode,
                table_key,
                table_conf
            )

            after_join_components[table_key] = components

        
        # ── Build Task ──────────────────────────────────────────
        task = DataFlowTask(
            audit=self.audit,
            registry=self.registry,
            before_join_components=before_join_components,
            join_task=join_task,
            after_join_components=after_join_components,
        )

        task.dataframe_dicts = dataframe_dicts
        # Print nicely
        print("\n=== DataFrame Dicts ===")
        for df in dataframe_dicts:
            print(f"Dimension: {df['dimension']}, Source: {df['source']}, File: {df['file_path']}")

        print("\n=== Before Join Components ===")
        for source, chain in before_join_components.items():
            chain_names = [c.__class__.__name__ for c in chain]
            print(f"Source: {source} → Chain: {chain_names}")

        print("\n=== Join Task ===")
        if join_task:
            print(f"Join task exists: {join_task.__class__.__name__}")
        else:
            print("No join task needed")

        print("\n=== After Join Components ===")
        for table_key, comps in after_join_components.items():
            comp_names = [c.__class__.__name__ for c in comps]
            print(f"Table: {table_key} → Components: {comp_names}")

        return task, dataframe_dicts
    # ══════════════════════════════════════════════════════════════════
    # STAGE BUILDERS
    # ══════════════════════════════════════════════════════════════════
    def _build_before_join_chain(self, file_key: str, file_path: str) -> list:

        chain = []

        # Read
        chain.append(
            ReadFromSourceFactory.create_source(
                file_name=file_path   
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
            chain.append(PIIMask(self.audit, self.registry))

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
                    foreign_keys=foreign_keys, #remove it
                    registry=self.registry,
                    audit=self.audit,
                )
            )

        # 3. Deduplicates
        components.append(
            DuplicatesLookUp(
                primary_key=primary_key, ##remove it
                table_key=table_key, ##remove it
                registry=self.registry,
                audit=self.audit,
            )
        )

        # 4. Load
        repo = self.registry.get_repository(table_key)

        components.append(
            LoadToTarget(
                source=table_key, ##remove it
                repo=repo, #remove it
                registry=self.registry,
                audit=self.audit,
                pk_column=primary_key,#remove it
            )
        )

        # 5.  OrphanLookUp ONLY for microbatch
        if batch_mode in ["micro_batch", "microbatch"] and foreign_keys:

            for fk_column, fk_conf in foreign_keys.items():

                dim_table = fk_conf.get("dim_table")
                pk_column = fk_conf.get("pk_column")

                components.append(
                    OrphanLookUp(
                        fk_column=fk_column,#remove it
                        dim_table=dim_table,#remove it
                        pk_column=pk_column,#remove it
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
