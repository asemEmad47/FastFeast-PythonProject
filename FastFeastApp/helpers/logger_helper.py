import os
import logging
from datetime import datetime, timezone


def setup_logger(mode: str, batch_date: str, hour: str | None = None):

    if mode == "micro_batch":
        hour = hour or datetime.now(timezone.utc).strftime("%H")
        log_file = f"{batch_date}-{hour}.log"
    else:
        log_file = f"{batch_date}.log"

    log_dir = f"../../logs/{mode}"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger("fastfeast.audit")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger