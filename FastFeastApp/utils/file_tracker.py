import os
import shutil

from watchdog.events import FileSystemEventHandler, FileCreatedEvent

class FileTracker(FileSystemEventHandler):

    def __init__(self, archive_dir , archive_dir_stream  , archive_dir_batch):
        super().__init__()
        self.processed_files: set[str] = set()
        self._micro_batch = None
        self._archive_dir = archive_dir
        self._archive_dir_stream = archive_dir_stream
        self._archive_dir_batch = archive_dir_batch


    def set_micro_batch(self, micro_batch):
        self._micro_batch = micro_batch
        
    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            return
        self._micro_batch._process_file(event.src_path) 

    def is_processed(self, file_path: str) -> bool:
        return file_path in self.processed_files

    def mark_processed(self, file_path: str):
        self.processed_files.add(file_path)

    def get_unprocessed_files(self, source_path: str):
        all_files = []
        for entry in os.listdir(source_path):
            entry_path = os.path.join(source_path, entry)
            if os.path.isdir(entry_path):
                if self.is_archived(entry_path, is_streaming=False):
                    shutil.rmtree(entry_path)
                    continue
                for f in os.listdir(entry_path):
                    file_path = os.path.join(entry_path, f)
                    if os.path.isfile(file_path):
                        all_files.append(file_path)
            elif os.path.isfile(entry_path):
                if self.is_archived(entry_path, is_streaming=False):
                    os.remove(entry_path)
                    continue
                all_files.append(entry_path)
        return [f for f in all_files if not self.is_processed(f)]

    def is_archived(self, path, is_streaming):
        name = os.path.basename(path)
        if is_streaming:
            date_dir = os.path.basename(os.path.dirname(path))
            dest = os.path.join(self._archive_dir, self._archive_dir_stream, date_dir, name)
        else:
            dest = os.path.join(self._archive_dir, self._archive_dir_batch, name)
        
        is_existing = os.path.exists(dest)
        
        if is_existing:
            print(f"File {path} is already archived at {dest}.")
        return is_existing

    def move_files_to_archive(self, source_path: str, is_streaming):
        if is_streaming:
            hour_dir = os.path.basename(source_path) # like "09" in the date dir in the stream dir
            date_dir = os.path.basename(os.path.dirname(source_path)) # like 2026-03-05 in the stream dir
            archive_subdir = os.path.join(self._archive_dir, self._archive_dir_stream, date_dir)
            os.makedirs(archive_subdir, exist_ok=True)
            dest = os.path.join(archive_subdir, hour_dir) 
            shutil.move(source_path, dest)  
        else:
            for date_entry in os.listdir(source_path): 
                date_path = os.path.join(source_path, date_entry)  # like 2026-03-05 in the batch dir
                archive_subdir = os.path.join(self._archive_dir, self._archive_dir_batch, date_entry) 
                os.makedirs(archive_subdir, exist_ok=True)
                if os.path.isdir(date_path):
                    for file in os.listdir(date_path):  
                        file_path = os.path.join(date_path, file) 
                        shutil.move(file_path, archive_subdir)
                    os.rmdir(date_path)

        self.processed_files.clear()