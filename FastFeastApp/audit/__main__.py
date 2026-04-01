# import time
# import pandas as pd
# from .audit import Audit


# # ─────────────────────────────────────────────
# # MIMIC STAGES
# # ─────────────────────────────────────────────

# def mimic_schema_validation(df: pd.DataFrame) -> tuple[bool, list[dict], pd.DataFrame]:
#     bad_schemas = []
#     for col in df.columns:
#         if df[col].dtype == object and col == "amount":
#             bad_schemas.append({
#                 "model": df.__class__.__name__,
#                 "reason": f"Column '{col}' type mismatch. Expected float, got object"
#             })
#     return (False, bad_schemas, df) if bad_schemas else (True, [], df)


# def mimic_rows_validation(df: pd.DataFrame, required: list[str]) -> tuple[bool, list[dict], pd.DataFrame, int]:
#     bad_rows = []
#     null_count = 0
#     bad_idx = df[df[required].isnull().any(axis=1)].index
#     for idx in bad_idx:
#         bad_rows.append({
#             "row":    df.loc[idx].to_string(),
#             "reason": f"Missing required fields: {[c for c in required if pd.isnull(df.loc[idx, c])]}"
#         })
#         null_count += 1
#     clean_df = df.drop(bad_idx)
#     return True, bad_rows, clean_df, null_count


# def mimic_duplicate_check(df: pd.DataFrame, key: str) -> tuple[bool, int, pd.DataFrame]:
#     dupes    = df[df.duplicated(subset=[key], keep="first")]
#     clean_df = df.drop_duplicates(subset=[key], keep="first")
#     return True, len(dupes), clean_df


# def mimic_orphan_check(df: pd.DataFrame, fk_col: str, valid_ids: set) -> tuple[bool, list[dict], pd.DataFrame]:
#     bad_rows = []
#     bad_idx  = df[~df[fk_col].isin(valid_ids)].index
#     for idx in bad_idx:
#         bad_rows.append({
#             "row":    df.loc[idx].to_string(),
#             "reason": fk_col
#         })
#     clean_df = df.drop(bad_idx)
#     return True, bad_rows, clean_df


# def mimic_dwh_load(df: pd.DataFrame) -> tuple[bool, int]:
#     return True, len(df)


# # ─────────────────────────────────────────────
# # PROCESS ONE FILE
# # ─────────────────────────────────────────────

# def process_file(audit: Audit, file_name: str, df: pd.DataFrame):

#     audit.reset_file()
#     audit.set_file(file_name)
#     audit.start_timer()

#     total = len(df)

#     # ── Stage 1: Schema Validation ──────────────
#     audit.reset_stage()
#     audit.current_stage = "schema_validation"
#     t0 = time.time()
#     ok, bad_schemas, df = mimic_schema_validation(df)
#     latency = int((time.time() - t0) * 1000)

#     for entry in bad_schemas:
#         audit.log_issue(
#             issue=entry["reason"],
#             value=None,
#             action="quarantined"
#         )
#     audit.track_metrics({
#         "total_records":     total,
#         "passed_count":      total - len(bad_schemas),
#         "quarantined_count": len(bad_schemas),
#     })
#     audit.log_file_result(total, total - len(bad_schemas), len(bad_schemas), latency, ok)

#     if not ok:
#         audit.log_pipeline_end()
#         return

#     # ── Stage 2: Rows Validation ─────────────────
#     audit.reset_stage()
#     audit.current_stage = "rows_validation"
#     t0 = time.time()
#     ok, bad_rows, df, null_count = mimic_rows_validation(df, required=["id", "name"])
#     latency = int((time.time() - t0) * 1000)

#     for entry in bad_rows:
#         audit.log_issue(
#             issue=entry["reason"],
#             value=entry["row"],
#             action="quarantined"
#         )
#     audit.track_metrics({
#         "quarantined_count": len(bad_rows),
#         "null_count":        null_count,
#         "passed_count":      len(df),
#     })
#     audit.log_file_result(total, len(df), len(bad_rows), latency, True)

#     # ── Stage 3: Duplicate Check ─────────────────
#     audit.reset_stage()
#     audit.current_stage = "duplicate_check"
#     t0 = time.time()
#     ok, dupes, df = mimic_duplicate_check(df, key="id")
#     latency = int((time.time() - t0) * 1000)

#     audit.track_metrics({
#         "duplicate_count":   dupes,
#         "quarantined_count": dupes,
#         "passed_count":      len(df),
#     })
#     audit.log_file_result(len(df) + dupes, len(df), dupes, latency, True)

#     # ── Stage 4: Orphan Check (micro_batch only) ──
#     if audit.mode == "micro_batch":
#         audit.reset_stage()
#         audit.current_stage = "orphan_check"
#         valid_customer_ids = {1, 2, 3, 4, 5}
#         t0 = time.time()
#         ok, bad_rows, df = mimic_orphan_check(df, fk_col="customer_id", valid_ids=valid_customer_ids)
#         latency = int((time.time() - t0) * 1000)

#         for entry in bad_rows:
#             audit.log_issue(
#                 issue="orphan_reference",
#                 value=entry["row"],
#                 action="quarantined"
#             )
#         audit.track_metrics({
#             "orphan_count":      len(bad_rows),
#             "quarantined_count": len(bad_rows),
#             "passed_count":      len(df),
#         })
#         audit.log_file_result(len(df) + len(bad_rows), len(df), len(bad_rows), latency, True)

#     # ── Stage 5: DWH Load ────────────────────────
#     audit.reset_stage()
#     audit.current_stage = "dwh_load"
#     t0 = time.time()
#     ok, loaded = mimic_dwh_load(df)
#     latency = int((time.time() - t0) * 1000)

#     audit.track_metrics({
#         "total_records": loaded,
#         "passed_count":  loaded,
#     })
#     audit.log_file_result(loaded, loaded, 0, latency, True)

#     audit.log_pipeline_end()


# # ─────────────────────────────────────────────
# # ORCHESTRATE
# # ─────────────────────────────────────────────

# def orchestrate(audit: Audit, files: dict[str, pd.DataFrame], batch_date: str, hour: str | None = None):
#     audit.start_batch(batch_date, hour)

#     for file_name, df in files.items():
#         process_file(audit, file_name, df)

#     audit.end_batch()


# # ─────────────────────────────────────────────
# # BATCH RUN
# # ─────────────────────────────────────────────

# def run_batch():
#     audit = Audit(mode="batch")

#     files = {
#         "customers.csv": pd.DataFrame([
#             {"id": 1, "name": "Alice",  "customer_id": 1},
#             {"id": 2, "name": None,     "customer_id": 2},
#             {"id": 3, "name": "Bob",    "customer_id": 3},
#             {"id": 3, "name": "Bob",    "customer_id": 3},
#             {"id": 4, "name": "Carol",  "customer_id": 4},
#         ]),
#         "drivers.csv": pd.DataFrame([
#             {"id": 1, "name": "Driver A", "customer_id": 1},
#             {"id": 2, "name": "Driver B", "customer_id": 2},
#             {"id": 3, "name": "Driver C", "customer_id": 3},
#         ]),
#         "restaurants.json": pd.DataFrame([
#             {"id": 1, "name": "Rest A", "customer_id": 1},
#             {"id": 2, "name": None,     "customer_id": 2},
#             {"id": 3, "name": "Rest C", "customer_id": 3},
#         ]),
#     }

#     orchestrate(audit, files, batch_date="2026-02-20")


# # ─────────────────────────────────────────────
# # MICRO-BATCH RUN
# # ─────────────────────────────────────────────

# def run_micro_batch():
#     audit = Audit(mode="micro_batch")

#     files = {
#         "orders.json": pd.DataFrame([
#             {"id": 1, "name": "Order A", "customer_id": 1},
#             {"id": 2, "name": "Order B", "customer_id": 99999},
#             {"id": 3, "name": "Order C", "customer_id": 2},
#             {"id": 4, "name": None,      "customer_id": 3},
#             {"id": 5, "name": "Order E", "customer_id": 3},
#         ]),
#         "tickets.csv": pd.DataFrame([
#             {"id": 1, "name": "Ticket A", "customer_id": 1},
#             {"id": 2, "name": "Ticket B", "customer_id": 88888},
#             {"id": 2, "name": "Ticket B", "customer_id": 88888},
#             {"id": 3, "name": "Ticket C", "customer_id": 3},
#         ]),
#         "ticket_events.json": pd.DataFrame([
#             {"id": 1, "name": "Event A", "customer_id": 1},
#             {"id": 2, "name": "Event B", "customer_id": 2},
#             {"id": 3, "name": "Event C", "customer_id": 3},
#         ]),
#     }

#     orchestrate(audit, files, batch_date="2026-02-20", hour="09")


# # ─────────────────────────────────────────────
# # MAIN
# # ─────────────────────────────────────────────

# if __name__ == "__main__":

#     print("\n" + "=" * 60)
#     print("  BATCH RUN")
#     print("=" * 60)
#     run_batch()

#     print("\n" + "=" * 60)
#     print("  MICRO-BATCH RUN")
#     print("=" * 60)
#     run_micro_batch()