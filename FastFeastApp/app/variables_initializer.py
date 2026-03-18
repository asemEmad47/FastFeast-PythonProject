"""
VariablesInitializer
Constructs every shared object exactly once at startup.
AppManager calls initialize_variables(), then reads the exposed attributes.
"""
from registry.conf_file_parser import ConfFileParser
from registry.data_registry import DataRegistry
from audit.audit import Audit
from utils.file_tracker import FileTracker


class VariablesInitializer:

    def __init__(self) -> None:
        self.parser:       ConfFileParser = None
        self.registry:     DataRegistry   = None
        self.audit:        Audit          = None
        self.file_tracker: FileTracker    = None

    def initialize_variables(self) -> None:
        self.parser = ConfFileParser()
        self.parser.parse("conf/pipeline.yaml")

        self.registry     = DataRegistry(self.parser)
        self.audit        = Audit()
        self.file_tracker = FileTracker()
