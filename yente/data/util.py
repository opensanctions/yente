import fingerprints
from normality import WS
from datetime import datetime
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator, List


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


def expand_names(names: List[str]) -> List[str]:
    """Expand names into normalized version."""
    expanded = set(names)
    for name in names:
        fp = fingerprints.generate(name)
        if fp is not None:
            expanded.add(fp)
    return list(expanded)


def tokenize_names(names: List[str]) -> List[str]:
    expanded = set()
    for name in names:
        name = name.lower()
        expanded.update(name.split(WS))
        fp = fingerprints.generate(name)
        if fp is not None:
            expanded.update(fp.split(WS))
    return list(expanded)


@asynccontextmanager
async def http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(
        total=3600,
        connect=None,
        sock_connect=None,
        sock_read=None,
    )
    async with ClientSession(timeout=timeout, trust_env=True) as client:
        yield client
