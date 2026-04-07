from db.database_manager import DatabaseManager
from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from audit.audit import Audit
from etl.tasks.email_task import EmailTask
from repository.base_repository import BaseRepository
from utils.file_tracker import FileTracker
from batch.batch import Batch
from batch.micro_batch import MicroBatch
from etl.workflow import WorkFlow

import os
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
        
        # Initialize registry and parser
        base_dir = os.path.dirname(__file__)
        pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
        batch_file    = os.path.abspath(os.path.join(base_dir, "..", "conf", "conf.yaml"))
        
        self.parser = ConfFileParser()
        self.registry = DataRegistry(self.parser)
        self.registry.load_config(batch_file)
        
        self.email_task = EmailTask()
        
        # File paths and batch interval
        self.micro_batch_path = self.registry.get_microbatch_path()
        self.batch_path = self.registry.get_batch_path()
        self.archive_dir = self.registry.get_archive_dir()
        self.archive_dir_stream = self.registry.get_archive_dir_stream()
        self.archive_dir_batch = self.registry.get_archive_dir_batch()
        self.batch_interval = int(self.registry.get_batch_interval())
        # ----------------------------------------------------------------  
        
        self.registry.load_config(pipeline_file)

        # Batch part
        batch_audit = Audit("batch")
        batch_workflow = WorkFlow("batch", self.registry, audit=batch_audit, alerter=None, validator=None)
        self.batch = Batch(
                batch_workflow,
                batch_audit,
                FileTracker(self.archive_dir,self.archive_dir_stream,self.archive_dir_batch), 
                self.registry, 
                self.parser, 
                self.email_task, 
                self.batch_path
            )
    
    
        # Micro-batch part
        micro_batch_audit = Audit("micro_batch")
        micro_batch_workflow = WorkFlow("micro_batch", self.registry,micro_batch_audit, alerter=None, validator=None)
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
        
        
        # Register repositories for all tables
        db = DatabaseManager()
        for table_key in self.registry.get_all_table_keys():
            self.registry.register_repository(
                table_key,
                BaseRepository(db,self.registry, table_key)
            )