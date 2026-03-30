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
        self.batch: Batch
        self.micro_batch: MicroBatch
        self.email_task: EmailTask
        
        self.batch_interval: int
        self.batch_path: str
        self.micro_batch_path: str
        self.archive_dir: str
        self.archive_dir_stream: str
        self.archive_dir_batch: str

    def initialize_variables(self) -> None:
        self.parser = ConfFileParser()
        self.registry = DataRegistry(self.parser)
        self.email_task = EmailTask()
        # self.parser.parse("conf/pipeline.yaml")
        
        # temp initilization (will comment after integrating the registry)
        self.micro_batch_path = "../../data/input/stream"
        self.batch_path = "../../data/input/batch"
        self.archive_dir = "../../data/archive"
        self.archive_dir_stream = "stream"
        self.archive_dir_batch = "batch"
        self.batch_interval = 20
        # ----------------------------------------------------------------  

        # Batch part
        batch_audit = Audit()
        batch_workflow = WorkFlow()
        self.batch = Batch(
                batch_workflow,
                batch_audit, 
                FileTracker(self.archive_dir,self.archive_dir_stream,self.archive_dir_batch), 
                self.registry, 
                self.parser, 
                self.email_task, 
                self.batch_path
            )
        # self.batch_interval = self.parser.get_batch_interval()
        # self.batch_path = self.parser.get_batch_path()
        # self.archive_dir = self.parser.get_archive_dir()
        # self.archive_dir_batch = self.parser.get_archive_dir_batch()
        # ----------------------------------------------------------------  
    
    
        # Micro-batch part
        micro_batch_audit = Audit()
        micro_batch_workflow = WorkFlow()
        micro_batch_file_tracker = FileTracker(self.archive_dir,self.archive_dir_stream,self.archive_dir_batch)
        self.micro_batch = MicroBatch(
            micro_batch_workflow,
            micro_batch_audit, 
            micro_batch_file_tracker,
            self.registry, 
            self.parser, 
            self.email_task, 
            self.micro_batch_path
        )
        micro_batch_file_tracker.set_micro_batch(self.micro_batch)
        # self.micro_batch.source_path = self.micro_batch_path   
        # self.archive_dir_stream = self.parser.get_archive_dir_stream()
        # self.batch.source_path = self.batch_path
