import codecs
from datetime import datetime
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator, List

from yente import settings


def iso_datetime(value: str) -> datetime:
    """Parse a second-precision ISO date time string."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def iso_to_version(value: str) -> str:
    dt = iso_datetime(value)
    return dt.strftime("%Y%m%d%H%M%S")


def expand_dates(dates: List[str]):
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)


@asynccontextmanager
async def http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(
        total=settings.HTTP_TIMEOUT * 100.0,
        connect=None,
        sock_connect=None,
        sock_read=None,
    )
    async with ClientSession(timeout=timeout, trust_env=True) as client:
        yield client
