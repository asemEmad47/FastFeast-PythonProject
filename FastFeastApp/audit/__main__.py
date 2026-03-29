import time
import pandas as pd
from .audit import Audit


# ─────────────────────────────────────────────
# MIMIC STAGES
# ─────────────────────────────────────────────

def mimic_schema_validation(df: pd.DataFrame) -> tuple[bool, list[dict], pd.DataFrame]:
    bad_schemas = []
    for col in df.columns:
        if df[col].dtype == object and col == "amount":
            bad_schemas.append({
                "model": df.__class__.__name__,
                "reason": f"Column '{col}' type mismatch. Expected float, got object"
            })
    return (False, bad_schemas, df) if bad_schemas else (True, [], df)


def mimic_rows_validation(df: pd.DataFrame, required: list[str]) -> tuple[bool, list[dict], pd.DataFrame]:
    bad_rows = []
    bad_idx  = df[df[required].isnull().any(axis=1)].index
    for idx in bad_idx:
        bad_rows.append({
            "row":    df.loc[idx].to_string(),
            "reason": f"Missing required fields: {[c for c in required if pd.isnull(df.loc[idx, c])]}"
        })
    clean_df = df.drop(bad_idx)
    return True, bad_rows, clean_df


def mimic_duplicate_check(df: pd.DataFrame, key: str) -> tuple[bool, int, pd.DataFrame]:
    dupes    = df[df.duplicated(subset=[key], keep="first")]
    clean_df = df.drop_duplicates(subset=[key], keep="first")
    return True, len(dupes), clean_df


def mimic_orphan_check(df: pd.DataFrame, fk_col: str, valid_ids: set) -> tuple[bool, list[dict], pd.DataFrame]:
    bad_rows = []
    bad_idx  = df[~df[fk_col].isin(valid_ids)].index
    for idx in bad_idx:
        bad_rows.append({
            "row":    df.loc[idx].to_string(),
            "reason": fk_col
        })
    clean_df = df.drop(bad_idx)
    return True, bad_rows, clean_df


def mimic_dwh_load(df: pd.DataFrame) -> tuple[bool, int]:
    return True, len(df)


# ─────────────────────────────────────────────
# PROCESS ONE FILE
# ─────────────────────────────────────────────

def process_file(audit: Audit, file_name: str, df: pd.DataFrame):

    audit.reset_file()               # wipe file counters
    audit.set_file(file_name)        # set file_name + run_id
    audit.start_timer()              # log PIPELINE_START

    total = len(df)

    # ── Stage 1: Schema Validation ──────────────
    audit.reset_stage()
    t0 = time.time()
    ok, bad_schemas, df = mimic_schema_validation(df)
    latency = int((time.time() - t0) * 1000)

    for entry in bad_schemas:
        audit.log_issue(
            stage="schema_validation",
            record_id=entry["model"],
            field="schema",
            issue=entry["reason"],
            value=None,
            action="quarantined"
        )
    audit.track_metrics({
        "total_records":     total,
        "passed_count":      total - len(bad_schemas),
        "quarantined_count": len(bad_schemas),
    })
    audit.log_file_result("schema_validation", total, total - len(bad_schemas), len(bad_schemas), latency, ok)

    if not ok:
        audit.log_pipeline_end()
        return

    # ── Stage 2: Rows Validation ─────────────────
    audit.reset_stage()
    t0 = time.time()
    ok, bad_rows, df = mimic_rows_validation(df, required=["id", "name"])
    latency = int((time.time() - t0) * 1000)

    for entry in bad_rows:
        audit.log_issue(
            stage="rows_validation",
            record_id="unknown",
            field="unknown",
            issue=entry["reason"],
            value=entry["row"],
            action="quarantined"
        )
    audit.track_metrics({
        "quarantined_count": len(bad_rows),
        "passed_count":      len(df),
        "null_counts":       {"required_field": sum(1 for e in bad_rows if "Missing" in e["reason"])}
    })
    audit.log_file_result("rows_validation", total, len(df), len(bad_rows), latency, True)

    # ── Stage 3: Duplicate Check ─────────────────
    audit.reset_stage()
    t0 = time.time()
    ok, dupes, df = mimic_duplicate_check(df, key="id")
    latency = int((time.time() - t0) * 1000)

    audit.track_metrics({
        "duplicate_count":   dupes,
        "quarantined_count": dupes,
        "passed_count":      len(df),
    })
    audit.log_file_result("duplicate_check", len(df) + dupes, len(df), dupes, latency, True)

    # ── Stage 4: Orphan Check (micro_batch only) ──
    if audit.mode == "micro_batch":
        audit.reset_stage()
        valid_customer_ids = {1, 2, 3, 4, 5}
        t0 = time.time()
        ok, bad_rows, df = mimic_orphan_check(df, fk_col="customer_id", valid_ids=valid_customer_ids)
        latency = int((time.time() - t0) * 1000)

        for entry in bad_rows:
            audit.log_issue(
                stage="orphan_check",
                record_id="unknown",
                field=entry["reason"],
                issue="orphan_reference",
                value=entry["row"],
                action="quarantined"
            )
        audit.track_metrics({
            "orphan_count":      len(bad_rows),
            "quarantined_count": len(bad_rows),
            "passed_count":      len(df),
        })
        audit.log_file_result("orphan_check", len(df) + len(bad_rows), len(df), len(bad_rows), latency, True)

    # ── Stage 5: DWH Load ────────────────────────
    audit.reset_stage()
    t0 = time.time()
    ok, loaded = mimic_dwh_load(df)
    latency = int((time.time() - t0) * 1000)

    audit.track_metrics({
        "total_records": loaded,
        "passed_count":  loaded,
    })
    audit.log_file_result("dwh_load", loaded, loaded, 0, latency, True)

    audit.log_pipeline_end()         # log file summary + accumulate to batch


# ─────────────────────────────────────────────
# ORCHESTRATE
# ─────────────────────────────────────────────

def orchestrate(audit: Audit, files: dict[str, pd.DataFrame]):
    audit.start_batch()

    for file_name, df in files.items():
        process_file(audit, file_name, df)

    audit.end_batch()


# ─────────────────────────────────────────────
# BATCH RUN
# ─────────────────────────────────────────────

def run_batch():
    audit = Audit(mode="batch", batch_date="2026-02-20")

    files = {
        "customers.csv": pd.DataFrame([
            {"id": 1, "name": "Alice",  "customer_id": 1},
            {"id": 2, "name": None,     "customer_id": 2},   # null name
            {"id": 3, "name": "Bob",    "customer_id": 3},
            {"id": 3, "name": "Bob",    "customer_id": 3},   # duplicate
            {"id": 4, "name": "Carol",  "customer_id": 4},
        ]),
        "drivers.csv": pd.DataFrame([
            {"id": 1, "name": "Driver A", "customer_id": 1},
            {"id": 2, "name": "Driver B", "customer_id": 2},
            {"id": 3, "name": "Driver C", "customer_id": 3},
        ]),
        "restaurants.json": pd.DataFrame([
            {"id": 1, "name": "Rest A", "customer_id": 1},
            {"id": 2, "name": None,     "customer_id": 2},   # null name
            {"id": 3, "name": "Rest C", "customer_id": 3},
        ]),
    }

    orchestrate(audit, files)


# ─────────────────────────────────────────────
# MICRO-BATCH RUN
# ─────────────────────────────────────────────

def run_micro_batch():
    audit = Audit(mode="micro_batch", batch_date="2026-02-20", hour="09")

    files = {
        "orders.json": pd.DataFrame([
            {"id": 1, "name": "Order A", "customer_id": 1},
            {"id": 2, "name": "Order B", "customer_id": 99999},  # orphan
            {"id": 3, "name": "Order C", "customer_id": 2},
            {"id": 4, "name": None,      "customer_id": 3},      # null name
            {"id": 5, "name": "Order E", "customer_id": 3},
        ]),
        "tickets.csv": pd.DataFrame([
            {"id": 1, "name": "Ticket A", "customer_id": 1},
            {"id": 2, "name": "Ticket B", "customer_id": 88888},  # orphan
            {"id": 2, "name": "Ticket B", "customer_id": 88888},  # duplicate
            {"id": 3, "name": "Ticket C", "customer_id": 3},
        ]),
        "ticket_events.json": pd.DataFrame([
            {"id": 1, "name": "Event A", "customer_id": 1},
            {"id": 2, "name": "Event B", "customer_id": 2},
            {"id": 3, "name": "Event C", "customer_id": 3},
        ]),
    }

    orchestrate(audit, files)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":

    print("\n" + "=" * 60)
    print("  BATCH RUN")
    print("=" * 60)
    run_batch()

    print("\n" + "=" * 60)
    print("  MICRO-BATCH RUN")
    print("=" * 60)
    run_micro_batch()