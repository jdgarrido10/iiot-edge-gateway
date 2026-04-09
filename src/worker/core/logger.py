"""
Structured logging system for production environments.
Supports JSON and plain-text output formats.
"""

import sys
import json
import logging
from datetime import datetime, timezone
from config import Config


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter for log aggregation pipelines."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread_id": record.thread,
            "thread_name": record.threadName,
        }

        if hasattr(record, "extra"):
            log_obj.update(record.extra)

        if record.exc_info:
            log_obj["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        return json.dumps(log_obj, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Coloured plain-text formatter for interactive development sessions."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[1;31m",  # Bold Red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        if sys.stderr.isatty():
            level = (
                f"{self.COLORS.get(record.levelname, '')}{record.levelname}{self.RESET}"
            )
        else:
            level = record.levelname

        output = (
            f"[{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}] "
            f"{level:8s} "
            f"{record.name:20s} "
            f"{record.getMessage()}"
        )

        if record.exc_info:
            output += f"\n{self.formatException(record.exc_info)}"

        return output


def setup_logger(name: str | None = None) -> logging.Logger:
    """Configure and return a named logger.

    Handlers are added only once; subsequent calls with the same name
    return the existing logger unchanged.

    Args:
        name: Logger name. Pass ``__name__`` from the calling module,
              or ``None`` for the root logger.

    Returns:
        Configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    log_level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    if Config.LOG_FORMAT == "json":
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = TextFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that merges a fixed context dict into every log record.

    Example::

        logger = LoggerAdapter(setup_logger(__name__), {"line": "Linea_0202"})
        logger.info("Workers updated", extra={"worker_count": 5})
    """

    def process(self, msg: str, kwargs: dict) -> tuple:
        kwargs.setdefault("extra", {}).update(self.extra)
        return msg, kwargs
