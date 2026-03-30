import time
import threading
from app.variables_initializer import VariablesInitializer

class AppManager:

    def __init__(self):
        self._initializer = VariablesInitializer()

    def initialize(self):
        self._initializer.initialize_variables()

    def start(self):
        init = self._initializer

        t_batch = threading.Thread(
            target=self._batch_loop,
            args=(init.batch, init.batch_interval),
            name="BatchThread",
            daemon=True, # To kill thread if the programm closes
        )
        t_micro = threading.Thread(
            target=init.micro_batch.run,
            name="MicroBatchThread",
            daemon=True,
        )

        t_batch.start()
        t_micro.start()
        t_batch.join() # To keep main thread alive while this thread is running
        t_micro.join() 

    def _batch_loop(self, batch, interval: int):
        while True:
            print("Running batch process...")
            batch.run()
            time.sleep(interval)
            
