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
from etl.components.transformer                 import Transformer
from etl.components.join                        import Join
from etl.components.load_to_target              import LoadToTarget
from etl.components.orphans_handler                 import OrphansHandler
from etl.lookup.duplicates_lookup               import DuplicatesLookUp
from etl.lookup.orphan_lookup                    import OrphanLookUp
from validation.rows_validator import RowsValidator
from validation.schema_validator import SchemaValidator
from etl.components.load_to_fact import LoadToFact
class DataFlowTasksCreator:

    def __init__(self,registry, audit, data):
        self.registry = registry
        self.audit = audit
        self.data = data

    # ═══════════════════════════════════════════════════════════════
    # PUBLIC METHOD
    # ═══════════════════════════════════════════════════════════════
    def create_data_flow_task(self,batch_mode: str,matched_data: list ) -> DataFlowTask:

        # ── Stage 1: dataframe_dicts ─────────────────────────────
        dataframe_dicts = []
        before_join_components = {}

        for item in matched_data:
            table_key = item["table"]
            sources = item["sources"]
            files = item["files"]

            for file_key in sources:
                file_name = self.registry.get_file_name(file_key)

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

            # build components for this table only
            components = self._build_after_join_components(
                batch_mode,
                table_key
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

        if(file_path is None):
            return chain
        # Read
        chain.append(
            ReadFromSourceFactory.create_source(
                file_name=file_path,
                audit=self.audit,
                registry=self.registry
            )
        )

        if self._is_static_and_processed(file_key):
            return chain
        
        # # Validate
        validator = ValidatorComponent(audit=self.audit, registry=self.registry)
        validator.register_validations(
            {
                "schema": SchemaValidator(),
                "rows": RowsValidator(),
            })
        chain.append(
            validator
        )
        # PII
        pii_fields = self.registry.get_pii_columns(file_key)
        if pii_fields:
            chain.append(PIIMask(self.audit, self.registry))

        return chain
    

    def _build_join(self) -> Join:


        return Join(
            audit=self.audit,
            registry=self.registry,
            )
        
    


    def _build_after_join_components(self, batch_mode, table_key):

        components = []

        # 1. Transformer
        components.append(
            Transformer( self.audit , self.registry)
        )

        # 3. Deduplicates
        components.append(
            DuplicatesLookUp(
                registry=self.registry,
                audit=self.audit,
            )
        )

        # if batch_mode in ["micro_batch", "microbatch"] :

        #     components.append(
        #         OrphanLookUp(
        #             registry=self.registry,
        #             audit=self.audit,
        #         )
                
        #     )
            
        #     components.append(
        #         LoadToFact(
        #             registry=self.registry,
        #             audit=self.audit,
        #         )
        #     )

        if batch_mode in ["batch"]:
            components.append(
                LoadToTarget(
                    registry=self.registry,
                    audit=self.audit,
                )
            )
            components.append(
                OrphansHandler(
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

    def _is_static_and_processed(self, file_key: str) -> bool:
        file_type = self.registry.get_file_type(file_key)

        if file_type == "static":
            return True

        return False