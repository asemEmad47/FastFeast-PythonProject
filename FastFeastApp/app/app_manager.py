"""
AppManager — Top-level facade.

1. Delegates object creation to VariablesInitializer.
2. Wires WorkFlow with all dependencies.
3. Starts Batch and MicroBatch on separate daemon threads.
"""
import threading
from app.variables_initializer import VariablesInitializer
from batch.batch import Batch
from batch.micro_batch import MicroBatch
from etl.workflow import WorkFlow
from audit.email_task import EmailTask


class AppManager:

    def __init__(self) -> None:
        self._initializer  = VariablesInitializer()
        self._batch:       Batch      = None
        self._micro_batch: MicroBatch = None

    # ------------------------------------------------------------------
    def initialize(self) -> None:
        self._initializer.initialize_variables()

        registry     = self._initializer.registry
        parser       = self._initializer.parser
        file_tracker = self._initializer.file_tracker
        audit        = self._initializer.audit

        workflow = WorkFlow()
        workflow.registry     = registry
        workflow.parser       = parser
        workflow.audit        = audit
        workflow.alerter      = EmailTask()
        workflow.file_tracker = file_tracker

        self._batch = Batch()
        self._batch.workflow      = workflow
        self._batch.file_tracker  = file_tracker

        self._micro_batch = MicroBatch()
        self._micro_batch.workflow     = workflow
        self._micro_batch.file_tracker = file_tracker

    # ------------------------------------------------------------------
    def start(self) -> None:
        t1 = threading.Thread(target=self._batch.run,       name="BatchThread",      daemon=True)
        t2 = threading.Thread(target=self._micro_batch.run, name="MicroBatchThread", daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
