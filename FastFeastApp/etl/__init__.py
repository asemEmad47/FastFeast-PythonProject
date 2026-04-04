
'''
import pandas as pd
from FastFeastApp.etl.data_flow_task import DataFlowTask

class FakeComponent:
    def __init__(self, name):
        self.name = name

    def do_task(self, data_dict):
        print(f"{self.name} running on {data_dict['source']}")

        metrics = {"rows": len(data_dict["dataframe"])}
        bad_rows = None

        return True, [], data_dict, metrics, bad_rows
    
class FakeJoin:
    def do_task(self, dataframe_dicts):
        print("Joining data...")

        try:
            df1 = dataframe_dicts[0]["dataframe"]
            df2 = dataframe_dicts[1]["dataframe"]

            merged = df1.merge(df2, on="id", how="inner")

            result = [{
                "dataframe": merged,
                "dimension": "joined",
                "source": "join_result"
            }]

            metrics = {"rows": len(merged)}
            bad_rows = None

            return True, [], result, metrics, bad_rows

        except Exception as e:
            return False, [str(e)], [], {}, None
            
class FakeAudit:
    def log_failure(self, msg):
        print("FAIL:", msg)

    def track_metrics(self, name, metrics):
        print(f"METRICS [{name}]:", metrics)

df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
df2 = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})

data = [
    {"dataframe": df1, "dimension": "customers", "source": "customers.csv"},
    {"dataframe": df2, "dimension": "orders", "source": "orders.csv"},
]

task = DataFlowTask(
    audit=FakeAudit(),
    registry=None,
    before_join_components={
        "customers.csv": [FakeComponent("Clean"), FakeComponent("Validate")],
        "orders.csv": [FakeComponent("CleanOrders")]
    },
    join_task=FakeJoin(),
    after_join_components=[FakeComponent("Transform"), FakeComponent("Load")]
)

success, errors = task.do_task(data)

print("SUCCESS:", success)
print("ERRORS:", errors)
'''


#####################################################
################### MAI #############################
#####################################################


import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from registry.conf_file_parser     import ConfFileParser
from registry.data_registry        import DataRegistry
from etl.data_flow_tasks_creator   import DataFlowTasksCreator
from etl.components.load_to_target import LoadToTarget
from etl.components.join           import Join

# ── 1. Setup parser and registry ──────────────────────────────────────
base_dir      = os.path.dirname(__file__)
pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
batch_file    = os.path.abspath(os.path.join(base_dir, "..", "conf", "conf.yaml"))

parser   = ConfFileParser()


registry = DataRegistry(parser)
registry.load_config(pipeline_file)

# ── 2. Pick a table to test ───────────────────────────────────────────
#table_key = "AgentsDim"

#table_conf = registry.get_table_conf(table_key)
#sources = registry.get_target_source(table_key)  # ["agents", "teams"]

# Simulate the files present in this run
fake_files = [
    "../../../FastFeast-Data/scripts/data/input/batch/2026-02-20/agents.csv",
    "../../../FastFeast-Data/scripts/data/input/batch/2026-02-20/teams.csv",
]

# ── 3. Build the creator ───────────────────────────────────────────────
all_tables = registry.get_all_table_keys()

for table_key in all_tables:

    print("\n" + "=" * 60)
    print(f"Processing Table: {table_key}")
    print("=" * 60)

    table_conf = registry.get_table_conf(table_key)
    sources = registry.get_target_source(table_key)

    creator = DataFlowTasksCreator(
        parser=parser,
        registry=registry,
        audit=None,
        files=fake_files,
    )

    task = creator.create_data_flow_task(
        batch_mode="batch",   
        table_key=table_key,
        table_conf=table_conf,
        active_sources=sources,



    )

    # ── Print ─────────────────────────────
    print("\nDataFrame Dicts:")
    for d in task.dataframe_dicts:
        print(d)

    print("\nBefore Join Components:")
    for k, v in task.before_join_components.items():
        print(k, "→", [type(c).__name__ for c in v])

    print("\nJoin Task:")
    print(type(task.join_task).__name__ if task.join_task else "None")

    print("\nAfter Join Components:")
    for c in task.after_join_components:
        print(type(c).__name__)


'''
# ── 5. Inspect the output ──────────────────────────────────────────────
print("=" * 55)
print(f"  DataFlowTask for: {table_key}")
print("=" * 55)

print("\n── data_framse_dicts ─────────────────────────────────")
for d in task.data_framse_dicts:
    print(f"  source={d['source']!r:20}  dimension={d['dimension']!r}  dataframe={d['dataframe']}")

print("\n── before_join_components ────────────────────────────")
for filename, chain in task.before_join_components.items():
    names = [type(c).__name__ for c in chain]
    print(f"  {filename!r:20} → {names}")

print("\n── join_task ─────────────────────────────────────────")
if task.join_task is None:
    print("  None (single source — no join needed)")
else:
    print(f"  {type(task.join_task).__name__}")
    for cfg in task.join_task.join_configs:
        print(f"    {cfg}")

print("\n── after_join_components ─────────────────────────────")
for c in task.after_join_components:
    print(f"  {type(c).__name__}")

print("\n── LoadToTarget always last? ─────────────────────────")
print(f"  {isinstance(task.after_join_components[-1], LoadToTarget)}  (expected: True)")

print("=" * 55)
'''




