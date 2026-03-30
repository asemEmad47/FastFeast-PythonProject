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

parser = ConfFileParser()
registry = DataRegistry(parser)

registry.load_config(pipeline_file)


# print("\n=== Full pipeline.yaml conf ===")
# pprint.pprint(registry._conf)

# # -----------------------------
# # Table config getters
# # -----------------------------
# print("\n=== Table Config Getters ===")

# tables_section = parser.get_all_tables_conf(registry._conf)
# print("All tables:", list(tables_section.keys()))

# for table_key in tables_section:
#     table_conf = registry.get_table_conf(table_key)

#     print(f"\nTable: {table_key}")
#     print("Table Configuration:", table_conf)

#     print("Target table name:", parser.get_target_table_name(table_conf))
#     print("Target table type:", parser.get_target_table_type(table_conf))
#     print("Primary key:", parser.get_target_primary_key(table_conf))
#     print("Foreign keys:", parser.get_target_foreign_keys(table_conf))
#     print("Required fields:", parser.get_target_required_fields(table_conf))
#     print("Source tables:", parser.get_target_source(table_conf))
#     print("Joins:", parser.get_join_config(table_conf))
#     print("Keep columns:", parser.get_target_columns(table_conf))
#     print("Fact dimension joins:", parser.get_fact_join_config(table_conf))
#     print("Fact aggregated columns:", parser.get_fact_aggregated_columns(table_conf))

# # -----------------------------
# # File config getters
# # -----------------------------
# print("\n=== File Config Getters ===")

# files_section = parser.get_workflow_files(registry._conf)

# for file_key in files_section:
#     file_conf = registry.get_file_conf(file_key)
#     print(f"\nFile key: {file_key}")
#     print(f"File config: {file_conf}")
#     print(f"File name: {parser.get_file_name(file_conf)}")
#     print(f"File type: {parser.get_file_type(file_conf)}")
#     print(f"Required fields: {parser.get_file_required_fields(file_conf)}")
#     print(f"File columns: {parser.get_file_columns(file_conf)}")

# # -----------------------------
# # Batch / microbatch / archive paths
# # -----------------------------
registry.load_config(batch_file)
batch_conf = registry._conf

# print("\n=== Full batch conf ===")
# pprint.pprint(batch_conf)

# print("\n=== Batch / Microbatch / Archive Paths ===")

# batch_section = batch_conf.get("batch", {})

# batch_path = parser.get_batch_path(batch_section)
# microbatch_path = parser.get_microbatch_path(batch_section)
# archive_path = parser.get_archive_dir(batch_section)

# print("Batch interval:", parser.get_batch_interval(batch_section))
# print("Batch path:", batch_path)
# print("Microbatch path:", microbatch_path)
# print("Archive dir:", archive_path)

# # Resolve paths
# def resolve_path(relative_path: str):
#     return os.path.abspath(os.path.join(base_dir, relative_path))

# batch_path = resolve_path(batch_path)
# microbatch_path = resolve_path(microbatch_path)
# archive_path = resolve_path(archive_path)

# print("Resolved Batch path:", batch_path)
# print("Resolved Microbatch path:", microbatch_path)
# print("Resolved Archive path:", archive_path)

# # List files
# def list_files_in_directory(path: str):
#     if not path or not os.path.exists(path):
#         print(f"Path does not exist: {path}")
#         return
#     print(f"\nFiles in: {path}")
#     for file in os.listdir(path):
#         print(" -", file)

# list_files_in_directory(batch_path)
# list_files_in_directory(microbatch_path)
# list_files_in_directory(archive_path)

# # -----------------------------
# # Archive (extra paths)
# # -----------------------------
# print("\n=== Archive Paths ===")

# archive_stream_path = parser.get_archive_dir_stream(batch_section)
# archive_batch_path = parser.get_archive_dir_batch(batch_section)

# print("Archive stream dir:", archive_stream_path)
# print("Archive batch dir:", archive_batch_path)

# archive_stream_path = resolve_path(archive_stream_path)
# archive_batch_path = resolve_path(archive_batch_path)

# list_files_in_directory(archive_stream_path)
# list_files_in_directory(archive_batch_path)

# # -----------------------------
# # DataRegistry Test
# # -----------------------------
registry.load_config(pipeline_file)

# print("\n=== DataRegistry Summary ===")
# registry.summary()