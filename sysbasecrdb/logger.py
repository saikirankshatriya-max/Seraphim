"""Structured logging setup for the migration toolkit."""

import logging
import os
from datetime import datetime


def setup_logging(log_dir: str, log_level: str = "INFO", verbose: bool = False) -> logging.Logger:
    """Configure logging to both console and a timestamped log file.

    Returns the root 'sysbasecrdb' logger.
    """
    level = logging.DEBUG if verbose else getattr(logging, log_level.upper(), logging.INFO)

    logger = logging.getLogger("sysbasecrdb")
    logger.setLevel(level)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(os.path.join(log_dir, f"migration_{ts}.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
