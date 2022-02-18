import sys
import logging
import structlog

from yente import settings


def configure_logging(level=logging.INFO):
    """Configure log levels and structured logging"""
    shared_processors = [
        structlog.processors.add_log_level,
        structlog.stdlib.add_logger_name,
        # structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        # structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.LOG_JSON:
        shared_processors.append(format_json)
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=structlog.processors.JSONRenderer(),
        )
    else:
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=structlog.dev.ConsoleRenderer(),
        )

    processors = shared_processors + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    # configuration for structlog based loggers
    structlog.configure(
        cache_logger_on_first_use=True,
        # wrapper_class=structlog.stdlib.AsyncBoundLogger,
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    es_logger = logging.getLogger("elasticsearch")
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


def format_json(_, __, ed):
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


class _MaxLevelFilter(object):
    def __init__(self, highest_log_level):
        self._highest_log_level = highest_log_level

    def filter(self, log_record):
        return log_record.levelno <= self._highest_log_level
