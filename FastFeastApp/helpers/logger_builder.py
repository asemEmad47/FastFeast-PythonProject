"""
helpers/logger_builder.py

Shared helper for building daily rotating file loggers
and providing standardized error logging.

Used by DatabaseManager and BaseRepository.
"""
from __future__ import annotations
import os
import logging
from logging.handlers import TimedRotatingFileHandler


def build_file_logger(name: str, log_dir: str) -> logging.Logger:
    """
    Builds and returns a named logger writing to log_dir/pipeline.log.
    Rotates daily at midnight UTC, retains 30 days of log files.
    Safe to call multiple times — handlers are never duplicated.

    Args:
        name:    Logger name e.g. 'fastfeast.db' or 'fastfeast.repository'
        log_dir: Absolute path to the logs/ directory for this module
    """
    os.makedirs(log_dir, exist_ok=True)

    _logger = logging.getLogger(name)
    if _logger.handlers:
        return _logger

    _logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        filename    = os.path.join(log_dir, "pipeline.log"),
        when        = "midnight",
        interval    = 1,
        backupCount = 30,
        encoding    = "utf-8",
        utc         = True
    )
    handler.suffix = "%Y-%m-%d"
    handler.setFormatter(logging.Formatter(
        fmt     = "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    ))

    _logger.addHandler(handler)
    _logger.propagate = False
    return _logger


def log_error(logger: logging.Logger, audit, message: str) -> None:
    """
    Logs an error to the module's rotating file logger
    and optionally forwards to the Audit instance.

    Args:
        logger:  The module-level logger built by build_file_logger()
        audit:   Audit instance or None
        message: The formatted error message to log
    """
    logger.error(message)
    if audit:
        audit.log_failure(message)