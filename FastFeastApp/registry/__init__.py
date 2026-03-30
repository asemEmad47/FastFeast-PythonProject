from conf_file_parser import ConfFileParser
import pprint
import os
import sys
from data_registry import DataRegistry

# Add FastFeastApp/ to Python path so 'models' package is found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

base_dir = os.path.dirname(__file__)
pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
batch_file    = os.path.abspath(os.path.join(base_dir, "..", "conf", "conf.yaml"))

# Initialize parser + registry
parser = ConfFileParser()
registry = DataRegistry(parser)

# -----------------------------
# Load pipeline config
# -----------------------------
registry.load_config(pipeline_file)

# print("\n=== Table Config Getters ===")

# all_tables_conf = registry.get_all_tables_conf()
# print("All tables:", list(all_tables_conf.keys()))

# for table_key in all_tables_conf:
#     table_conf = registry.get_table_conf(table_key)

#     print(f"\nTable: {table_key}")
#     print("Table Configuration:", table_conf)

#     print("Target table name:", registry.get_target_table_name(table_key))
#     print("Target table type:", registry.get_target_table_type(table_key))
#     print("Primary key:", registry.get_target_primary_key(table_key))
#     print("Foreign keys:", registry.get_target_foreign_keys(table_key))
#     print("Required fields:", registry.get_target_required_fields(table_key))
#     print("Source tables:", registry.get_target_source(table_key))
#     print("Joins:", registry.get_join_config(table_key))
#     print("Keep columns:", registry.get_dimension_columns(table_key))
#     print("Fact dimension joins:", registry.get_fact_join_config(table_key))
#     print("Fact aggregated columns:", registry.get_aggregated_columns(table_key))

# -----------------------------
# File Config Getters
# -----------------------------
# print("\n=== File Config Getters ===")

# workflow_files = registry.get_workflow_files()  
# for file_key in workflow_files:
#     file_conf = registry.get_file_conf(file_key)

#     print(f"\nFile key: {file_key}")
#     print(f"File config: {file_conf}")
#     print(f"File name: {registry.get_file_name(file_key)}")
#     print(f"File type: {registry.get_file_type(file_key)}")
#     print(f"Required fields: {registry.get_file_required_fields(file_key)}")
#     print(f"PII columns: {registry.get_pii_columns(file_key)}")

# -----------------------------
# Load batch config
# -----------------------------
registry.load_config(batch_file)
batch_section = registry.get_all_batch_conf()

# print("\n=== Full Batch Config ===")
# pprint.pprint(batch_section)

# print("\n=== Batch / Microbatch / Archive Paths ===")

# batch_path = registry.get_batch_path()
# microbatch_path = registry.get_microbatch_path()
# archive_path = registry.get_archive_dir()

# print("Batch interval:", registry.get_batch_interval())
# print("Batch path:", batch_path)
# print("Microbatch path:", microbatch_path)
# print("Archive dir:", archive_path)

# # Resolve paths
# def resolve_path(path: str):
#     return os.path.abspath(os.path.join(base_dir, path))

# batch_path = resolve_path(batch_path)
# microbatch_path = resolve_path(microbatch_path)
# archive_path = resolve_path(archive_path)

# print("Resolved Batch path:", batch_path)
# print("Resolved Microbatch path:", microbatch_path)
# print("Resolved Archive path:", archive_path)

# List files
# def list_files(path: str):
#     if not path or not os.path.exists(path):
#         print(f"Path does not exist: {path}")
#         return
#     print(f"\nFiles in: {path}")
#     for f in os.listdir(path):
#         print(" -", f)

# list_files(batch_path)
# list_files(microbatch_path)
# list_files(archive_path)

# -----------------------------
# Archive extra paths
# -----------------------------
# print("\n=== Archive Paths ===")

# archive_stream_path = registry.get_archive_dir_stream()
# archive_batch_path = registry.get_archive_dir_batch()

# archive_stream_path = resolve_path(archive_stream_path)
# archive_batch_path = resolve_path(archive_batch_path)

# print("Archive stream dir:", archive_stream_path)
# print("Archive batch dir:", archive_batch_path)

# list_files(archive_stream_path)
# list_files(archive_batch_path)

# # -----------------------------
# # DataRegistry Summary
# # -----------------------------
registry.load_config(pipeline_file)
# print("\n=== DataRegistry Summary ===")
# registry.summary()