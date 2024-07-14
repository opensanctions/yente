import asyncio
from typing import cast
from structlog.contextvars import get_contextvars

from yente import settings
from yente.logs import get_logger


log = get_logger(__name__)
query_semaphore = asyncio.Semaphore(settings.QUERY_CONCURRENCY)


def get_opaque_id() -> str:
    ctx = get_contextvars()
    return cast(str, ctx.get("trace_id"))
