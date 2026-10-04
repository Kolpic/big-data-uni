"""
Logging configuration for the whole project.
Used by the generator, the Dask/Spark/Ray stages and the report builder.

Every logger writes to BOTH the console (real-time feedback) and a log file
(so the run can be inspected afterwards) - the same pattern used across the
course exercises.
"""

import logging
import sys
from pathlib import Path


def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Create and configure a logger for a given module.

    Args:
        name: Logger name (usually __name__).
        log_file: Optional path/name of a file to also write the logs to.

    Returns:
        A configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if the logger is requested twice.
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    # Do not propagate to the root logger (avoids duplicated console output
    # once libraries such as Ray install their own root handler).
    logger.propagate = False

    # Format: time | level | module | message
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) Console handler - immediate visibility.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2) File handler - optional, for later inspection.
    if log_file:
        # Keep every log file inside the project's logs/ directory.
        log_path = Path(__file__).parent / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path / log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
