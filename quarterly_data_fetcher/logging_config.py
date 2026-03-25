"""
Centralized logging configuration for structured JSON logging with Loki support.
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_fields"):
            log_obj.update(record.extra_fields)

        return json.dumps(log_obj)


class StructuredLogger(logging.Logger):
    """Custom logger that supports structured logging with extra fields."""

    def log_with_context(
        self,
        level: int,
        message: str,
        extra_fields: Dict[str, Any] = None,
        **kwargs,
    ):
        """Log with additional context fields."""
        if extra_fields is None:
            extra_fields = {}

        record = self.makeRecord(
            self.name,
            level,
            kwargs.get("pathname", ""),
            kwargs.get("lineno", 0),
            message,
            (),
            None,
        )
        record.extra_fields = extra_fields
        self.handle(record)

    def info_with_context(self, message: str, extra_fields: Dict[str, Any] = None):
        self.log_with_context(logging.INFO, message, extra_fields)

    def error_with_context(self, message: str, extra_fields: Dict[str, Any] = None):
        self.log_with_context(logging.ERROR, message, extra_fields)

    def debug_with_context(self, message: str, extra_fields: Dict[str, Any] = None):
        self.log_with_context(logging.DEBUG, message, extra_fields)


def setup_logging(
    name: str = "quarterly-data-fetcher",
    level: int = logging.INFO,
    use_json: bool = True,
) -> logging.Logger:
    """Configure structured logging with JSON formatter."""
    logging.setLoggerClass(StructuredLogger)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if use_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    log_dir = "/var/log/app"
    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(f"{log_dir}/{name}.log")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except (OSError, IOError) as error:
        console_handler.handleError(
            logging.makeLogRecord(
                {
                    "msg": f"Failed to set up file logging: {error}",
                    "levelno": logging.WARNING,
                }
            )
        )

    return logging.getLogger(name)
