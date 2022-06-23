from asyncstdlib import contextmanager
import orjson
import aiofiles
from pathlib import Path
from aiocsv import AsyncDictReader
from pydantic import AnyHttpUrl, FileUrl
from typing import Any, AsyncGenerator, Dict, Union

from yente import settings
from yente.logs import get_logger
from yente.data.util import http_session

ENCODING = "utf-"
URL = Union[AnyHttpUrl, FileUrl]
BUFFER = 10 * 1024 * 1024

log = get_logger(__name__)


@contextmanager
async def cached_url(url: URL, base_name: str) -> AsyncGenerator[Path, None]:
    if isinstance(url, FileUrl):
        if url.path is None:
            raise ValueError("Invalid path: %s" % url)
        yield Path(url.path).resolve()
        return

    out_path = settings.DATA_PATH.joinpath(base_name)
    try:
        async with http_session() as client:
            log.info("Fetching data", url=url, path=out_path.as_uri())
            async with client.get(str(url)) as resp:
                async with aiofiles.open(out_path, "wb") as outfh:
                    while chunk := await resp.content.read(BUFFER):
                        await outfh.write(chunk)
        yield out_path
    finally:
        out_path.unlink(missing_ok=True)


async def load_json_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def load_csv_rows(path: Path) -> AsyncGenerator[Dict[str, str], None]:
    async with aiofiles.open(path, "r", encoding="utf8") as fh:
        async for row in AsyncDictReader(fh):
            yield row
    return
