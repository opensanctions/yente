import sys
import logging
import structlog
from logging import Filter, LogRecord
from typing import cast, Any, Dict, List
from structlog.dev import ConsoleRenderer, set_exc_info
from structlog.contextvars import merge_contextvars
from structlog.processors import UnicodeDecoder, TimeStamper
from structlog.processors import format_exc_info, add_log_level
from structlog.processors import JSONRenderer
from structlog.stdlib import ProcessorFormatter, add_logger_name
from structlog.stdlib import BoundLogger, LoggerFactory
from structlog.stdlib import get_logger as get_raw_logger

from yente import settings


def get_logger(name: str) -> BoundLogger:
    return get_raw_logger(name)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure log levels and structured logging"""
    shared_processors: List[Any] = [
        add_log_level,
        add_logger_name,
        # structlog.stdlib.PositionalArgumentsFormatter(),
        # structlog.processors.StackInfoRenderer(),
        merge_contextvars,
        set_exc_info,
        TimeStamper(fmt="iso"),
        # format_exc_info,
        UnicodeDecoder(),
    ]

    if settings.LOG_JSON:
        shared_processors.append(format_exc_info)
        shared_processors.append(format_json)
        formatter = ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=JSONRenderer(),
        )
    else:
        formatter = ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=ConsoleRenderer(
                exception_formatter=structlog.dev.plain_traceback
            ),
        )

    processors = shared_processors + [
        ProcessorFormatter.wrap_for_formatter,
    ]

    # configuration for structlog based loggers
    structlog.configure(
        cache_logger_on_first_use=True,
        # wrapper_class=AsyncBoundLogger,
        wrapper_class=BoundLogger,
        processors=processors,
        context_class=dict,
        logger_factory=LoggerFactory(),
    )

    es_logger = logging.getLogger("elastic_transport")
    es_logger.setLevel(logging.WARNING)

    uv_logger = logging.getLogger("uvicorn")
    uv_logger.handlers = []

    uv_access = logging.getLogger("uvicorn.access")
    uv_access.handlers = []
    uv_access.setLevel(logging.WARNING)
    uv_access.propagate = True

    # handler for low level logs that should be sent to STDOUT
    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setLevel(level)
    out_handler.addFilter(_MaxLevelFilter(logging.WARNING))
    out_handler.setFormatter(formatter)
    # handler for high level logs that should be sent to STDERR
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.LOG_LEVEL)
    root_logger.addHandler(out_handler)
    root_logger.addHandler(error_handler)


def format_json(_: Any, __: Any, ed: Dict[str, str]) -> Dict[str, str]:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


class _MaxLevelFilter(Filter):
    def __init__(self, highest_log_level: int) -> None:
        self._highest_log_level = highest_log_level

    def filter(self, log_record: LogRecord) -> bool:
        return log_record.levelno <= self._highest_log_level
