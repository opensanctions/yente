import json
from aiocsv import AsyncDictReader
from typing import Any, AsyncGenerator, Dict
from aiohttp import ClientSession, ClientTimeout

from yente.data.util import AsyncTextReaderWrapper

http_timeout = ClientTimeout(
    total=3600 * 6,
    connect=None,
    sock_read=None,
    sock_connect=None,
)


async def load_json_lines(url: str) -> AsyncGenerator[Any, None]:
    async with ClientSession(timeout=http_timeout) as client:
        async with client.get(url) as resp:
            async for line in resp.content:
                yield json.loads(line)


async def load_csv_rows(url: str) -> AsyncGenerator[Dict[str, str], None]:
    async with ClientSession(timeout=http_timeout) as client:
        async with client.get(url) as resp:
            wrapper = AsyncTextReaderWrapper(resp.content, "utf-8")
            async for row in AsyncDictReader(wrapper):
                yield row
