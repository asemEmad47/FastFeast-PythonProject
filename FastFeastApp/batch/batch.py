"""
Batch — Daily full-load thread.
Scans batch source folder, skips already-processed files, runs WorkFlow.
"""
import os
from config.settings import BATCH_SOURCE_DIR
from etl.workflow import WorkFlow
from utils.file_tracker import FileTracker


class Batch:

    def __init__(self) -> None:
        self.workflow:     WorkFlow    = None
        self.file_tracker: FileTracker = None

    def run(self) -> None:
        files = self._get_files()
        if not files:
            return
        self.workflow.files = files
        self.workflow.orchestrate()

    def _get_files(self) -> list[str]:
        all_files = [
            os.path.join(BATCH_SOURCE_DIR, f)
            for f in os.listdir(BATCH_SOURCE_DIR)
            if os.path.isfile(os.path.join(BATCH_SOURCE_DIR, f))
        ]
        return [f for f in all_files if not self.file_tracker.is_processed(f)]
