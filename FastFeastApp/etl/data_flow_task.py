from __future__ import annotations
from audit.audit import Audit
from etl.task import Task
from registry.data_registry import DataRegistry
from etl.components.quarantine_writer import QuarantineWriter
from etl.components.join import Join
from utils.dataframe_parser import DataFrameParser
class DataFlowTask(Task):

    def __init__(self, audit : Audit, registry : DataRegistry,   before_join_components=None, join_task=Join, after_join_components=None):
        self.audit = audit
        self.registry = registry
        self.before_join_components = before_join_components or {}
        self.join_task = join_task
        self.after_join_components = after_join_components or []
        self.quarantine_writer = QuarantineWriter(audit=self.audit, registry=self.registry)

    # ══════════════════════════════════════════════════════════════════
    # PUBLIC — called by WorkFlow.orchestrate()
    # ══════════════════════════════════════════════════════════════════
    def do_task(self, dataframe_dicts) -> tuple[bool, list[str]]:
        all_errors: list[str] = []

        # ── Stage 1: before-join ──────────────────────────────────────
        for i, data_dict in enumerate(dataframe_dicts):
            source = data_dict.get("source")

            if not source:
                self.audit.log_failure("Missing source key in data_dict")
                continue

            chain = self.before_join_components.get(source, [])

            if not chain:
                self.audit.log_failure(f"No before-join components found for source: {source}")
                continue

            self.audit.set_file(source) 
            self.audit.start_timer()  
            
            file_ok = True

            for comp in chain:
                ok, errors, data_dict, metrics, bad_rows = comp.do_task(data_dict)
                
                if bad_rows is not None and not bad_rows.empty:
                    bad_dict = {
                        "dataframe": bad_rows,
                        "dimension": data_dict.get("dimension")
                    }
                    self.quarantine_writer.set_errors(errors)
                    _, q_errors, _, _, _ = self.quarantine_writer.do_task(bad_dict)
                    all_errors.extend(q_errors)
                    
                all_errors.extend(errors)

                self.audit.track_metrics(metrics)

                if not ok:
                    self.audit.log_failure(f"Before-join component failed [{source}]: {errors}")
                    file_ok = False
                    break

            dataframe_dicts[i] = data_dict

            # Log end of this file's pipeline
            # schema_failed=True only if the very first component (Reader) failed
            self.audit.log_pipeline_end(schema_failed=not file_ok)

        # ── Stage 2: join ────────────────────────────────────────────
        if self.join_task and dataframe_dicts:
            self.join_task.set_data_framse_dict(dataframe_dicts)

            print(self.join_task.__class__.__name__)
            
            ok, errors, result_dicts, metrics, bad_rows = self.join_task.do_task(dataframe_dicts)
            
            dataframe_dicts[:] = [
                d for d in dataframe_dicts
                if self.registry.get_target_table_type(d["dimension"]) != "static_dimension"
            ]
            
            if bad_rows is not None and not bad_rows.empty:
                bad_dict = {
                    "dataframe": bad_rows,
                    "dimension": data_dict.get("dimension")
                }
                self.quarantine_writer.set_errors(errors)
                _, q_errors, _, _, _ = self.quarantine_writer.do_task(bad_dict)
                all_errors.extend(q_errors)
            all_errors.extend(errors)

            self.audit.set_file("join_stage")
            self.audit.track_metrics(metrics)

            if not ok:
                self.audit.log_failure(f"Join stage failed: {errors}")
                self.audit.log_pipeline_end(schema_failed=True)
                return False, all_errors

            dataframe_dicts = result_dicts
            
            
        for src in dataframe_dicts:
            date_cols = self.registry.get_target_date_columns(src["dimension"])
            records = (
                DataFrameParser(src["dataframe"])
                    .normalize_timestamps(date_columns=date_cols)
                    .fill_nulls()
                    .to_df()
            )
            src["dataframe"] = records
            
            
            

        # ── Stage 3: after-join ───────────────────────────────────────
        if not dataframe_dicts:
            return True, all_errors

        for i, data_dict in enumerate(dataframe_dicts):
            dimension = data_dict["dimension"]
            working = data_dict

            comps = self.after_join_components.get(dimension, [])
            if not comps:
                self.audit.log_failure(f"No after-join components found for dimension: {dimension}")
                continue

            self.audit.set_file(dimension)
            self.audit.start_timer()

            dim_ok = True

            for comp in comps:
                print(f"[After-Join] Running: {comp.__class__.__name__} for {dimension}")

                ok, errors, working, metrics, bad_rows = comp.do_task(working)
                
                if bad_rows is not None and not bad_rows.empty:
                    bad_dict = {
                        "dataframe": bad_rows,
                        "dimension": data_dict.get("dimension"),
                    }
                    self.quarantine_writer.set_errors(errors)
                    _, q_errors, _, _, _ = self.quarantine_writer.do_task(bad_dict)
                    all_errors.extend(q_errors)
                    
                all_errors.extend(errors)

                self.audit.track_metrics(metrics)

                if not ok:
                    self.audit.log_failure(f"After-join component failed [{dimension}]: {errors}")
                    dim_ok = False
                    break

            dataframe_dicts[i] = working
            self.audit.log_pipeline_end(schema_failed=not dim_ok)

        return True, all_errors