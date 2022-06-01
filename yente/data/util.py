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
    http_timeout = ClientTimeout(total=settings.HTTP_TIMEOUT)
    async with ClientSession(timeout=http_timeout, trust_env=True) as client:
        yield client


class AsyncTextReaderWrapper:
    # from: https://github.com/MKuranowski/aiocsv/issues/2#issuecomment-706554973
    def __init__(self, obj, encoding, errors="strict"):
        self.obj = obj

        decoder_factory = codecs.getincrementaldecoder(encoding)
        self.decoder = decoder_factory(errors)

    async def read(self, size):
        raw_data = await self.obj.read(size)

        if not raw_data:
            return self.decoder.decode(b"", final=True)

        return self.decoder.decode(raw_data, final=False)
