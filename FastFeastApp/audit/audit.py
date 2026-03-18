"""
Audit — Metrics accumulator + logger.

Holds running numeric counters per pipeline run.
Resets at the start of every orchestrate() call.
Persists one row to pipeline_run_log after each run via persist_to_dwh().

Two outputs:
  1. DWH pipeline_run_log table  — queryable, analytics-facing (persist_to_dwh)
  2. Structured log file on disk — for engineers debugging in real time (log_success / log_failure)
"""
import logging
import uuid
from datetime import datetime
from typing import Any

logger = logging.getLogger("fastfeast.audit")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")


class Audit:

    def __init__(self) -> None:
        self.reset()

    # ------------------------------------------------------------------
    def reset(self) -> None:
        self.total_records:    int         = 0
        self.null_counts:      dict        = {}
        self.duplicate_count:  int         = 0
        self.orphan_count:     int         = 0
        self.quarantined_count:int         = 0
        self.scd_inserted:     int         = 0
        self.scd_updated:      int         = 0
        self.scd_expired:      int         = 0
        self.scd_skipped:      int         = 0
        self._start_time:      datetime    = None
        self._metrics_log:     list[dict]  = []

    def start_timer(self) -> None:
        self._start_time = datetime.utcnow()

    # ------------------------------------------------------------------
    def track_metrics(self, component: str, stats: dict[str, Any]) -> None:
        """Accumulate metrics from any pipeline component."""
        self._metrics_log.append({"component": component, **stats})

        if "total_records"    in stats: self.total_records     += stats["total_records"]
        if "duplicate_count"  in stats: self.duplicate_count   += stats["duplicate_count"]
        if "orphan_count"     in stats: self.orphan_count       += stats["orphan_count"]
        if "quarantined_count"in stats: self.quarantined_count  += stats["quarantined_count"]
        if "scd_inserted"     in stats: self.scd_inserted       += stats["scd_inserted"]
        if "scd_updated"      in stats: self.scd_updated        += stats["scd_updated"]
        if "scd_expired"      in stats: self.scd_expired        += stats["scd_expired"]
        if "scd_skipped"      in stats: self.scd_skipped        += stats["scd_skipped"]
        if "null_counts"      in stats:
            for col, cnt in stats["null_counts"].items():
                self.null_counts[col] = self.null_counts.get(col, 0) + cnt

    # ------------------------------------------------------------------
    def get_summary(self) -> dict:
        elapsed_ms = int((datetime.utcnow() - self._start_time).total_seconds() * 1000) if self._start_time else 0
        return {
            "total_records":    self.total_records,
            "null_counts":      self.null_counts,
            "duplicate_count":  self.duplicate_count,
            "orphan_count":     self.orphan_count,
            "quarantined_count":self.quarantined_count,
            "scd_inserted":     self.scd_inserted,
            "scd_updated":      self.scd_updated,
            "scd_expired":      self.scd_expired,
            "scd_skipped":      self.scd_skipped,
            "processing_latency_ms": elapsed_ms,
        }

    # ------------------------------------------------------------------
    def persist_to_dwh(self, run_log_repo) -> None:
        """Write one row to pipeline_run_log per run."""
        if run_log_repo is None:
            return
        summary = self.get_summary()
        total   = max(self.total_records, 1)

        record = {
            "run_id":                 str(uuid.uuid4()),
            "file_name":              "run",
            "total_records":          summary["total_records"],
            "null_rate":              round(sum(summary["null_counts"].values()) / total, 4),
            "orphan_rate":            round(self.orphan_count / total, 4),
            "processing_latency_ms":  summary["processing_latency_ms"],
            "scd_inserted_count":     self.scd_inserted,
            "scd_updated_count":      self.scd_updated,
            "scd_expired_count":      self.scd_expired,
            "file_success":           True,
        }
        try:
            run_log_repo.add_many([record])
        except Exception as exc:
            logger.error(f"Audit.persist_to_dwh failed: {exc}")

    # ------------------------------------------------------------------
    def log_success(self, message: str) -> None:
        logger.info(f"SUCCESS | {message}")

    def log_failure(self, message: str) -> None:
        logger.error(f"FAILURE | {message}")
