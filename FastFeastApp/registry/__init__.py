from conf_file_parser import ConfFileParser
import pprint
import os

base_dir = os.path.dirname(__file__)
pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
batch_file    = os.path.abspath(os.path.join(base_dir, "..", "conf", "conf.yaml"))

parser = ConfFileParser()

# Parse the two files separately
pipeline_conf = parser.parse(pipeline_file)       # pipeline.yaml
batch_conf    = parser.parse_batch_conf(batch_file)

pipeline_conf = parser.parse(pipeline_file)
batch_conf = parser.parse_batch_conf(batch_file)

# print("\n=== Full pipeline.yaml conf ===")
# pprint.pprint(pipeline_conf)

# print("\n=== Full batch conf ===")
# pprint.pprint(batch_conf)

# # -----------------------------
# # Table config getters
# # -----------------------------
# print("\n=== Table Config Getters ===")
# tables = parser.get_all_tables_conf()
# print("All tables:", list(tables.keys()))

# for table_key in tables:
#     conf_section = parser.get_table_conf(table_key)
#     print(f"\nTable: {table_key}")
#     print("Table Configuration: ", conf_section)
#     print("Target table name:", parser.get_target_table_name(conf_section))
#     print("Target table type:", parser.get_target_table_type(conf_section))
#     print("Primary key:", parser.get_target_primary_key(conf_section))
#     print("Foreign keys:", parser.get_target_foreign_keys(conf_section))
#     print("Required fields:", parser.get_target_required_fields(conf_section))
#     print("Source tables:", parser.get_target_source(conf_section))
#     print("Joins:", parser.get_join_config(conf_section))
#     print("keep_coloumns:", parser.get_target_columns(conf_section))
#     print("Fact dimension joins:", parser.get_fact_join_config(conf_section))
#     print("Fact aggregated columns:", parser.get_fact_aggregated_columns(conf_section))

# -----------------------------
# File config getters
# -----------------------------
# print("\n=== File Config Getters ===")
# workflow_files = parser.get_workflow_files()  

# for file_key in workflow_files:
#     file_conf = parser.get_file_conf(file_key)  
#     name = parser.get_file_name(file_conf)
#     type = parser.get_file_type(file_conf)
#     required_fields = parser.get_file_required_fields(file_conf)
#     columns = parser.get_file_columns(file_conf)
#     print(f"File key: {file_key}")
#     print(f"File config: {file_conf}")
#     print(f"File name: {name}")
#     print(f"File type: {type}")
#     print(f"Required fields: {required_fields}")
#     print(f"File columns: {columns}")

# # -----------------------------
# # Batch / microbatch / archive paths
# # -----------------------------
# print("\n=== Batch / Microbatch / Archive Paths ===")

# batch_path = parser.get_batch_path()
# microbatch_path = parser.get_microbatch_path()
# archive_path = parser.get_archive_dir()

# print("Batch interval:", parser.get_batch_interval())
# print("Batch path:", batch_path)
# print("Microbatch path:", microbatch_path)
# print("Archive dir:", archive_path)

# import os

# def resolve_path(relative_path: str):
#     base_dir = os.path.dirname(__file__)
#     return os.path.abspath(os.path.join(base_dir, relative_path))

# batch_path = resolve_path(parser.get_batch_path())
# microbatch_path = resolve_path(parser.get_microbatch_path())
# archive_path = resolve_path(parser.get_archive_dir())

# print("Resolved Batch path:", batch_path)
# print("Resolved Microbatch path:", microbatch_path)
# print("Resolved Archive path:", archive_path)

# def list_files_in_directory(path: str):
#     if not path or not os.path.exists(path):
#         print(f"Path does not exist: {path}")
#         return
#     print(f"\nFiles in: {path}")
#     for file in os.listdir(path):
#         full_path = os.path.join(path, file)
#         print(" -", file)

# list_files_in_directory(batch_path)
# list_files_in_directory(microbatch_path)
# list_files_in_directory(archive_path)

################################################
# test batch methods
################################################
# archive_stream_path = parser.get_archive_dir_stream()
# archive_batch_path = parser.get_archive_dir_batch()

# print("Archive stream dir:", archive_stream_path)
# print("Archive batch dir:", archive_batch_path)

# archive_stream_path = resolve_path(parser.get_archive_dir_stream())
# archive_batch_path = resolve_path(parser.get_archive_dir_batch())

# list_files_in_directory(archive_stream_path)
# list_files_in_directory(archive_batch_path)


###############################################
# mai test your data registry code under this comment
###############################################

