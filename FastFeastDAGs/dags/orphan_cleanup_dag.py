"""
orphan_cleanup_dag.py

Runs every 2 days.
Deletes unresolved orphan records older than 2 days
from QUARANTINE.OrphanRecords.
Logs results to dags/logs/orphan_cleanup_<run_id>.txt per run.
"""
from __future__ import annotations
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from include.scripts.orphan_log import log_results


# ── Constants ─────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_fastfeast"
SQL_PATH          = os.path.join(
                        os.path.dirname(__file__),
                        "..", "include", "sql", "orphan_cleanup.sql"
                    )

# ── Load SQL ──────────────────────────────────────────────────────────────────
with open(SQL_PATH, "r") as f:
    CLEANUP_SQL = f.read()

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id            = "orphan_cleanup_dag",
    description       = "Deletes unresolved orphan records older than 2 days",
    schedule          = timedelta(days=2),
    start_date        = datetime(2026, 1, 1),
    catchup           = False,
    default_args      = {
        "owner":            "fastfeast",
        "retries":          1,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags = ["fastfeast", "quarantine", "orphan"]
) as dag:

    cleanup_orphans = SQLExecuteQueryOperator(
        task_id           = "cleanup_orphans",
        conn_id           = SNOWFLAKE_CONN_ID,
        sql               = CLEANUP_SQL,
        do_xcom_push      = True
    )

    log_run_results = PythonOperator(
        task_id         = "log_run_results",
        python_callable = log_results,
    )

    cleanup_orphans >> log_run_results