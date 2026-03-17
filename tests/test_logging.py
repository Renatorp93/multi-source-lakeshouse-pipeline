import logging
from pathlib import Path

from lakehouse.common.logging import configure_logging, get_logger


def test_get_logger_returns_named_logger():
    test_logs_root = Path("./logs/test_logging")
    configure_logging(logs_root=str(test_logs_root))

    logger = get_logger("lakehouse.test")

    assert isinstance(logger, logging.Logger)
    assert logger.name == "lakehouse.test"
