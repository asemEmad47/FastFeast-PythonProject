from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from audit.audit import Audit
from audit.email_task import EmailTask
from utils.file_tracker import FileTracker
from batch.batch import Batch
from batch.micro_batch import MicroBatch
from etl.workflow import WorkFlow


class VariablesInitializer:

    def __init__(self) -> None:
        self.parser: ConfFileParser
        self.registry: DataRegistry
        self.audit: Audit
        self.batch_file_tracker: FileTracker
        self.micro_batch_file_tracker: FileTracker
        self.workflow: WorkFlow
        self.batch: Batch
        self.micro_batch: MicroBatch
        self.batch_interval: int
        self.batch_path: str
        self.micro_batch_path: str
        self.archive_dir: str

    def initialize_variables(self) -> None:
        self.parser = ConfFileParser()
        self.parser.parse("conf/pipeline.yaml")

        self.registry = DataRegistry(self.parser)
        self.audit = Audit()

        self.batch_interval = self.parser.get_batch_interval()
        self.batch_path = self.parser.get_batch_path()
        self.micro_batch_path = self.parser.get_micro_batch_path()
        self.archive_dir = self.parser.get_archive_dir()

        self.micro_batch = MicroBatch()
        self.micro_batch_file_tracker = FileTracker(self.micro_batch, self.archive_dir)
        self.micro_batch.file_tracker = self.micro_batch_file_tracker
        self.micro_batch.source_path = self.micro_batch_path

        self.batch = Batch()
        self.batch_file_tracker = FileTracker(self.batch, self.archive_dir)
        self.batch.file_tracker = self.batch_file_tracker
        self.batch.source_path = self.batch_path

        self.workflow = WorkFlow()
        self.workflow.registry = self.registry
        self.workflow.parser = self.parser
        self.workflow.audit = self.audit
        self.workflow.alerter = EmailTask()

        self.micro_batch.workflow = self.workflow
        self.batch.workflow = self.workflow