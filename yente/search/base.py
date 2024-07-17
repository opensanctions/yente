import asyncio
from typing import Any
from structlog.contextvars import get_contextvars

from yente import settings
from yente.logs import get_logger


log = get_logger(__name__)
query_semaphore = asyncio.Semaphore(settings.QUERY_CONCURRENCY)


def get_trace_id() -> Any:
    ctx = get_contextvars()
    return ctx.get("trace_id")
