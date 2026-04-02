from abc import ABC, abstractmethod
from etl.tasks.email_task import EmailTask
from etl.workflow import WorkFlow
from audit.audit import Audit
from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from utils.file_tracker import FileTracker


class BatchReader(ABC):

    def __init__(self,work_flow: WorkFlow , audit: Audit , file_tracker: FileTracker  , registry: DataRegistry , parser: ConfFileParser , alerter: EmailTask , source_path: str ):
        self.work_flow = work_flow 
        self.file_tracker = file_tracker 
        self.source_path = source_path
        
        work_flow.registry = registry
        work_flow.parser = parser
        work_flow.audit = audit
        work_flow.alerter = alerter
        
    @abstractmethod
    def run(self):
        """Start / execute the reader's processing cycle."""
        ...
            