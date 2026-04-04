from etl.tasks.email_task import EmailTask
from batch.batch_reader import BatchReader
from etl.workflow import WorkFlow
from audit.audit import Audit
from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from utils.file_tracker import FileTracker


class Batch(BatchReader):

    def __init__(self, work_flow: WorkFlow, audit: Audit, file_tracker: FileTracker , registry: DataRegistry, parser: ConfFileParser, alerter: EmailTask , source_path: str):
        super().__init__(work_flow=work_flow, audit=audit, file_tracker=file_tracker, registry=registry, parser=parser, alerter=alerter, source_path=source_path)

    def run(self) -> None:
        files = self.file_tracker.get_unprocessed_files(self.source_path)
        if not files:
            print("No new files to process.")
            return
        self.work_flow.files = files
        try:
            # self.work_flow.orchestrate() --> will uncomment this line when the workflow is implemented
            for f in files:
                self.file_tracker.mark_processed(f)
            self.file_tracker.move_files_to_archive(self.source_path, False)
            print("Batch processing completed.")
        except Exception as e:
            print(f"Error processing batch: {e}")
            raise