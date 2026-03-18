"""
MicroBatch — Near-real-time streaming thread.
Polls the stream source folder at a configured interval.
"""
import os
import time
from config.settings import MICROBATCH_SOURCE_DIR, MICROBATCH_POLL_INTERVAL_SEC
from etl.workflow import WorkFlow
from utils.file_tracker import FileTracker


class MicroBatch:

    def __init__(self) -> None:
        self.workflow:     WorkFlow    = None
        self.file_tracker: FileTracker = None

    def run(self) -> None:
        while True:
            files = self._get_new_files()
            if files:
                self.workflow.files = files
                self.workflow.orchestrate()
            time.sleep(MICROBATCH_POLL_INTERVAL_SEC)

    def _get_new_files(self) -> list[str]:
        all_files = [
            os.path.join(MICROBATCH_SOURCE_DIR, f)
            for f in os.listdir(MICROBATCH_SOURCE_DIR)
            if os.path.isfile(os.path.join(MICROBATCH_SOURCE_DIR, f))
        ]
        return [f for f in all_files if not self.file_tracker.is_processed(f)]
