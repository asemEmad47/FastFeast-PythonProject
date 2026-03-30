import os
import shutil
import threading

from audit.email_task import EmailTask
from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from watchdog.observers import Observer

from batch.batch_reader import BatchReader
from etl.workflow import WorkFlow
from audit.audit import Audit
from utils.file_tracker import FileTracker


class MicroBatch(BatchReader):

    def __init__(self, work_flow: WorkFlow, audit: Audit, file_tracker: FileTracker, registry: DataRegistry, parser: ConfFileParser, alerter: EmailTask, source_path: str):
        super().__init__(work_flow=work_flow, audit=audit, file_tracker=file_tracker, registry=registry, parser=parser, alerter=alerter, source_path=source_path)
        self._observer: Observer
        self._pending: dict[str, threading.Timer] = {}
        self._lock = threading.Lock() 

    def run(self):
        print("Starting micro-batch processing...")
        self._observer = Observer()
        self._observer.schedule(self.file_tracker, path=self.source_path, recursive=True)
        self._observer.start()
        try:
            self._observer.join()
        except KeyboardInterrupt:
            self._observer.stop()
            self._observer.join()

    def _process_file(self, path: str):

        print(f"Detected new file: {path}")
        hour_path = os.path.dirname(path)  

        with self._lock:
            if hour_path in self._pending:
                self._pending[hour_path].cancel()

            timer = threading.Timer(2.0, self._process_hour_dir, args=[hour_path])
            self._pending[hour_path] = timer
            timer.start()

    def _process_hour_dir(self, path: str):

        with self._lock:
            self._pending.pop(path, None)

        print(f"Processing hour directory: {path}")

        if self.file_tracker.is_processed(path):
            return

        if self.file_tracker.is_archived(path, True):
            print(f"Already archived {path}. Removing source.")
            shutil.rmtree(path)
            return

        files = []
        for f in os.listdir(path):
            file_path = os.path.join(path, f)
            if os.path.isfile(file_path):
                files.append(file_path)

        if not files:
            print(f"No files found in {path}. Skipping.")
            return

        print(f"Found files: {files}")
        self.work_flow.files = files
        try:
            # self.work_flow.orchestrate() --> will be uncommented after integrating with registery
            self.file_tracker.mark_processed(path)
            self.file_tracker.move_files_to_archive(path, True)
            print(f"Finished processing {path}")
        except Exception as e:
            print(f"Error processing {path}: {e}")
            raise