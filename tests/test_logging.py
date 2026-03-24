from lakehouse.common.logging import configure_logging, get_logger


def test_configure_logging_makes_project_logger_available():
    test_logs_root = "./logs/test_logging"
    configure_logging(logs_root=str(test_logs_root))

    logger = get_logger("lakehouse.test")

    assert logger.name == "lakehouse.test"
    assert logger.isEnabledFor(20)
