from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path


def configure_logging(log_level: str = "INFO", logs_root: str = "./logs") -> None:
    """Configura logging padrao para jobs locais."""
    Path(logs_root).mkdir(parents=True, exist_ok=True)

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": log_level,
                    "formatter": "standard",
                }
            },
            "root": {
                "handlers": ["console"],
                "level": log_level,
            },
        }
    )


def get_logger(name: str) -> logging.Logger:
    """Retorna logger padronizado do projeto."""
    return logging.getLogger(name)
