"""
orphan_log.py

Logging utility for the orphan cleanup DAG.
Called by PythonOperator after the cleanup task completes.
"""
from __future__ import annotations
import os
from datetime import datetime, timezone


LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs")


def log_results(**context) -> None:
    """
    Pulls XCom results from the cleanup task and writes
    a structured log file per DAG run.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    run_id         = context["run_id"]
    logical_dt     = context["logical_date"]                          # ← was execution_date
    cleanup_result = context["ti"].xcom_pull(task_ids="cleanup_orphans")

    log_path = os.path.join(LOG_DIR, f"orphan_cleanup_{run_id}.txt")

    with open(log_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("  FastFeast — Orphan Cleanup DAG Run Log\n")
        f.write("=" * 60 + "\n")
        f.write(f"  Run ID         : {run_id}\n")
        f.write(f"  Logical Date   : {logical_dt}\n")               # ← label updated too
        f.write(f"  Log Written At : {datetime.now(timezone.utc).isoformat()}\n")  # ← was utcnow()
        f.write(f"  Status         : SUCCESS\n")
        f.write(f"  Result         : {cleanup_result}\n")
        f.write("=" * 60 + "\n")

    print(f"Log written to: {log_path}")