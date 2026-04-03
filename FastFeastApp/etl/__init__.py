# import pandas as pd
# from FastFeastApp.etl.data_flow_task import DataFlowTask

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

# print("SUCCESS:", success)
# print("ERRORS:", errors)