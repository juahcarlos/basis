"""Configure application-wide logging."""

import logging.config

from app.config import Settings


def configure_logging(settings: Settings) -> None:
    """Set the root logger level from application settings."""
    # Keep third-party loggers enabled while routing application output consistently.
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"}
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                }
            },
            "root": {"level": settings.log_level.upper(), "handlers": ["console"]},
        }
    )
