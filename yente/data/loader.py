import json
import aiofiles
from aiocsv import AsyncDictReader
from pydantic import AnyHttpUrl, FileUrl
from typing import Any, AsyncGenerator, Dict, Union
from aiohttp import ClientSession, ClientTimeout

from yente.data.util import AsyncTextReaderWrapper

ENCODING = "utf-"
URL = Union[AnyHttpUrl, FileUrl]

http_timeout = ClientTimeout(
    total=3600 * 6,
    connect=None,
    sock_read=None,
    sock_connect=None,
)


async def load_json_lines(url: URL) -> AsyncGenerator[Any, None]:
    if isinstance(url, FileUrl):
        async with aiofiles.open(url.path, "r", encoding="utf8") as fh:
            async for file_line in fh:
                yield json.loads(file_line)
        return
    async with ClientSession(timeout=http_timeout) as client:
        async with client.get(str(url)) as resp:
            async for line in resp.content:
                yield json.loads(line)


async def load_csv_rows(url: URL) -> AsyncGenerator[Dict[str, str], None]:
    if isinstance(url, FileUrl):
        async with aiofiles.open(url.path, "r", encoding="utf8") as fh:
            async for row in AsyncDictReader(fh):
                yield row
        return
    async with ClientSession(timeout=http_timeout) as client:
        async with client.get(str(url)) as resp:
            wrapper = AsyncTextReaderWrapper(resp.content, "utf-8")
            async for row in AsyncDictReader(wrapper):
                yield row
