
'''
import pandas as pd
from FastFeastApp.etl.data_flow_task import DataFlowTask

# class FakeComponent:
#     def __init__(self, name):
#         self.name = name

#     def do_task(self, data_dict):
#         print(f"{self.name} running on {data_dict['source']}")

#         metrics = {"rows": len(data_dict["dataframe"])}
#         bad_rows = None

#         return True, [], data_dict, metrics, bad_rows
    
# class FakeJoin:
#     def do_task(self, dataframe_dicts):
#         print("Joining data...")

#         try:
#             df1 = dataframe_dicts[0]["dataframe"]
#             df2 = dataframe_dicts[1]["dataframe"]

#             merged = df1.merge(df2, on="id", how="inner")

#             result = [{
#                 "dataframe": merged,
#                 "dimension": "joined",
#                 "source": "join_result"
#             }]

#             metrics = {"rows": len(merged)}
#             bad_rows = None

#             return True, [], result, metrics, bad_rows

#         except Exception as e:
#             return False, [str(e)], [], {}, None
            
# class FakeAudit:
#     def log_failure(self, msg):
#         print("FAIL:", msg)

#     def track_metrics(self, name, metrics):
#         print(f"METRICS [{name}]:", metrics)

# df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
# df2 = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})

# data = [
#     {"dataframe": df1, "dimension": "customers", "source": "customers.csv"},
#     {"dataframe": df2, "dimension": "orders", "source": "orders.csv"},
# ]

# task = DataFlowTask(
#     audit=FakeAudit(),
#     registry=None,
#     before_join_components={
#         "customers.csv": [FakeComponent("Clean"), FakeComponent("Validate")],
#         "orders.csv": [FakeComponent("CleanOrders")]
#     },
#     join_task=FakeJoin(),
#     after_join_components=[FakeComponent("Transform"), FakeComponent("Load")]
# )

# success, errors = task.do_task(data)

print("SUCCESS:", success)
print("ERRORS:", errors)
'''


#####################################################
################### MAI #############################
#####################################################
'''

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
    ".../agents.csv",
    ".../teams.csv",
    ".../customers.csv",
    ".../segments.csv",
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
        registry=registry,
        audit=None,
        sources= sources,
        files=fake_files,
    )

    task = creator.create_data_flow_task(
        batch_mode="batch",   
        table_key=table_key,
        table_conf=table_conf,
        active_sources=sources,
        files=fake_files



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

######################################################
########### Workflow Test#############################
######################################################

import sys
import os
from pathlib import Path

# add project root to sys.path
sys.path.append(str(Path(__file__).parents[1]))

from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from etl.workflow import WorkFlow

base_dir = os.path.dirname(__file__)
pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))

# Initialize parser + registry
parser = ConfFileParser()
registry = DataRegistry(parser)
registry.load_config(pipeline_file)
workFlow = WorkFlow("batch", registry, audit=None, alerter=None,validator=None)

files = [
    "C:/data/customers.csv",
    "C:/data/agents.csv",
    "C:/data/orders.json",
    "C:/data/tickets.csv",
    "C:/data/ticket_events.json",
    "C:/data/drivers.csv",
    "C:/data/segments.csv",
    "C:/data/teams.csv",
    "C:/data/regions.csv",
    "C:/data/cities.csv",
]

workFlow.orchestrate(files)
