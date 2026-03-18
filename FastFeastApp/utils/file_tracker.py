"""
FileTracker — Idempotency guard.

Remembers which files have been processed so re-runs never double-load.
In-memory for now; swap _processed_files for a DB-backed set in production.
"""
import os
import shutil
from config.settings import ARCHIVE_DIR


class FileTracker:

    def __init__(self) -> None:
        self._processed_files: set[str] = set()

    def is_processed(self, file_path: str) -> bool:
        return file_path in self._processed_files

    def mark_processed(self, file_path: str) -> None:
        self._processed_files.add(file_path)

    def move_files_to_archive(self, file_paths: list[str]) -> None:
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        for path in file_paths:
            try:
                dest = os.path.join(ARCHIVE_DIR, os.path.basename(path))
                shutil.move(path, dest)
            except Exception:
                pass
