import uuid
from datetime import datetime, timezone
from typing import Any
from helpers.logger_helper import setup_logger


class Audit:

    def __init__(self, mode: str, batch_date: str, hour: str | None = None):
        self.mode = mode
        self.batch_date = batch_date
        self.logger = setup_logger(mode, batch_date, hour)
        self.reset_batch()
        self.reset_file()
        self.reset_stage()

    # region reset methods

    def reset_batch(self):
        self.batch_total_records: int = 0
        self.batch_passed_count: int = 0
        self.batch_duplicate_count: int = 0
        self.batch_orphan_count: int = 0
        self.batch_quarantined_count: int = 0
        self.batch_files_processed: int = 0
        self.batch_files_failed: int = 0 
        self.batch_files_with_issues: int = 0 
        self._batch_start_time: datetime | None = None

    def reset_file(self):
        self.file_name: str | None = None
        self.run_id: str | None = None
        self._file_start_time: datetime | None = None
        self.file_total_records: int = 0
        self.file_passed_count: int = 0
        self.file_duplicate_count: int = 0
        self.file_orphan_count: int = 0
        self.file_quarantined_count: int = 0

    def reset_stage(self):
        self.current_stage_in_records: int = 0
        self.current_stage_null_records: dict = {}
        self.current_stage_duplicate_records: int = 0
        self.current_stage_orphan_records: int = 0
        self.current_stage_quarantined_records: int = 0
        self.current_stage_passed_records: int = 0
        self.is_current_stage_alerted: bool = False

    # endregion

    # region batch methods

    def start_batch(self):
        self._batch_start_time = datetime.now(timezone.utc)
        self.logger.info(
            f"BATCH_START | mode={self.mode} | batch_date={self.batch_date}"
        )

    def end_batch(self):
        elapsed_ms = 0
        if self._batch_start_time:
            elapsed_ms = int(
                (datetime.now(timezone.utc) - self._batch_start_time).total_seconds() * 1000
            )
        
        total = max(self.batch_total_records, 1)
        
        batch_msg = (
            f"BATCH_END | mode={self.mode} | batch_date={self.batch_date} "
            f"| files_processed={self.batch_files_processed} "
            f"| files_failed={self.batch_files_failed} "
            f"| files_with_issues={self.batch_files_with_issues} "
            f"| total_records={self.batch_total_records} "
            f"| passed={self.batch_passed_count} | passed_rate={round((self.batch_passed_count / total) * 100, 2)}% "
            f"| duplicate={self.batch_duplicate_count} | duplicate_rate={round((self.batch_duplicate_count / total) * 100, 2)}% "
            f"| quarantined={self.batch_quarantined_count} | quarantine_rate={round((self.batch_quarantined_count / total) * 100, 2)}% "
        )
        
        if self.mode == "micro_batch":
            batch_msg += f"| orphans={self.batch_orphan_count} | orphan_rate={round((self.batch_orphan_count / total) * 100, 2)}% "
            
        batch_msg += f"| latency={elapsed_ms}ms"
        self.logger.info(batch_msg)

    def get_batch_summary(self) -> dict:
        elapsed_ms = 0
        if self._batch_start_time:
            elapsed_ms = int(
                (datetime.now(timezone.utc) - self._batch_start_time).total_seconds() * 1000
            )
        total = max(self.batch_total_records, 1)
        return {
            "mode": self.mode,
            "batch_date": self.batch_date,
            "files_processed": self.batch_files_processed,
            "files_failed": self.batch_files_failed,
            "files_with_issues": self.batch_files_with_issues,
            "total_records": self.batch_total_records,
            "total_passed": self.batch_passed_count,
            "total_duplicate": self.batch_duplicate_count,
            "total_orphans": self.batch_orphan_count,
            "total_quarantined": self.batch_quarantined_count,
            "duplicate_rate": round(self.batch_duplicate_count / total, 4),
            "orphan_rate": round(self.batch_orphan_count / total, 4),
            "quarantine_rate": round(self.batch_quarantined_count / total, 4),
            "referential_integrity_rate": round(self.batch_passed_count / total, 4),
            "processing_latency_ms": elapsed_ms,
        }

    def _accumulate_to_batch(self, schema_failed: bool = False):
        self.batch_total_records += self.file_total_records
        self.batch_passed_count += self.file_passed_count
        self.batch_duplicate_count += self.file_duplicate_count
        self.batch_orphan_count += self.file_orphan_count
        self.batch_quarantined_count += self.file_quarantined_count
        self.batch_files_processed += 1

        if schema_failed:
            self.batch_files_failed += 1
        elif self.file_quarantined_count > 0:
            self.batch_files_with_issues += 1

    # endregion

    # region file methods

    def start_timer(self):
        self._file_start_time = datetime.now(timezone.utc)
        self.logger.info(
            f"PIPELINE_START | run_id={self.run_id} | mode={self.mode} "
            f"| batch_date={self.batch_date} | file={self.file_name}"
        )

    def track_metrics(self, stats: dict[str, Any]):
        if "total_records" in stats: self.current_stage_in_records += stats["total_records"]
        if "passed_count" in stats: self.current_stage_passed_records += stats["passed_count"]
        if "duplicate_count" in stats: self.current_stage_duplicate_records += stats["duplicate_count"]
        if "orphan_count" in stats: self.current_stage_orphan_records += stats["orphan_count"]
        if "quarantined_count" in stats: self.current_stage_quarantined_records += stats["quarantined_count"]
        if "null_counts" in stats:
            for col, cnt in stats["null_counts"].items():
                self.current_stage_null_records[col] = self.current_stage_null_records.get(col, 0) + cnt

        if self.file_total_records == 0 and self.current_stage_in_records > 0:
            self.file_total_records = self.current_stage_in_records
            self.file_passed_count = self.current_stage_in_records

        self.file_duplicate_count += self.current_stage_duplicate_records
        self.file_orphan_count += self.current_stage_orphan_records
        self.file_quarantined_count += self.current_stage_quarantined_records
        self.file_passed_count -= self.current_stage_quarantined_records

        self.reset_stage()

    def log_pipeline_end(self, schema_failed: bool = False):
        elapsed_ms = 0
        if self._file_start_time:
            elapsed_ms = int(
                (datetime.now(timezone.utc) - self._file_start_time).total_seconds() * 1000
            )

        total_file = max(self.file_total_records, 1)

        log_msg = (
            f"PIPELINE_END | run_id={self.run_id} | file={self.file_name} "
            f"| total={self.file_total_records} "
            f"| passed={self.file_passed_count} "
            f"| duplicate={self.file_duplicate_count} "
            f"| duplicate_rate={round((self.file_duplicate_count / total_file) * 100, 2)}% "
        )
        if self.mode == "micro_batch":
            log_msg += (
                f"| orphan={self.file_orphan_count} "
                f"| orphan_rate={round((self.file_orphan_count / total_file) * 100, 2)}% "
                f"| bad_rows={self.file_quarantined_count - self.file_duplicate_count - self.file_orphan_count} "
                f"| bad_rows_rate={round(((self.file_quarantined_count - self.file_duplicate_count - self.file_orphan_count) / total_file) * 100, 2)}% "
            )
        else:
            log_msg += (
                f"| bad_rows={self.file_quarantined_count - self.file_duplicate_count} "
                f"| bad_rows_rate={round(((self.file_quarantined_count - self.file_duplicate_count) / total_file) * 100, 2)}% "
            )

        log_msg += (
            f"| total_quarantined={self.file_quarantined_count} "
            f"| quarantine_rate={round((self.file_quarantined_count / total_file) * 100, 2)}% "
            f"| schema_failed={schema_failed} "
            f"| latency={elapsed_ms}ms "
            f"| alert_sent={self.is_current_stage_alerted}"
        )
        self.logger.info(log_msg)
        self._accumulate_to_batch(schema_failed)

    # endregion

    # region stage methods

    def log_issue(self, stage: str, record_id: Any, field: str, issue: str, value: Any, action: str):
        self.logger.warning(
            f"ISSUE | stage={stage} | file={self.file_name} "
            f"| record={record_id} | field={field} "
            f"| issue={issue} | value={value} | action={action}"
        )

    def log_file_result(self, stage: str, records_in: int, records_ok: int, records_bad: int, latency_ms: int, success: bool):
        status = "OK" if success else "FAILED"
        log_fn = self.logger.info if success else self.logger.error
        log_fn(
            f"FILE_{status} | stage={stage} | file={self.file_name} "
            f"| in={records_in} ok={records_ok} bad={records_bad} "
            f"| latency={latency_ms}ms"
        )

    def mark_alert_sent(self):
        self.is_current_stage_alerted = True

    # endregion

    # region logging message

    def log_success(self, message: str):
        self.logger.info(f"SUCCESS | {message}")

    def log_failure(self, message: str):
        self.logger.error(f"FAILURE | {message}")

    # endregion

    def set_file(self, file_name: str):
        self.file_name = file_name
        self.run_id = str(uuid.uuid4())