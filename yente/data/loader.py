import orjson
import aiofiles
import structlog
from pathlib import Path
from aiocsv import AsyncDictReader
from pydantic import AnyHttpUrl, FileUrl
from structlog.stdlib import BoundLogger
from typing import Any, AsyncGenerator, Dict, Union

from yente import settings
from yente.data.util import AsyncTextReaderWrapper, http_session

ENCODING = "utf-"
URL = Union[AnyHttpUrl, FileUrl]

log: BoundLogger = structlog.get_logger(__name__)


async def fetch_url(url: URL, base_name: str) -> Path:
    if isinstance(url, FileUrl):
        if url.path is None:
            raise ValueError("Invalid path: %s" % url)
        return Path(url.path).resolve()

    out_path = settings.DATA_PATH.joinpath(base_name)
    async with http_session() as client:
        log.info("Fetching data", url=url, path=out_path.as_uri())
        async with client.get(str(url)) as resp:
            async with aiofiles.open(out_path, "wb") as outfh:
                while chunk := await resp.content.read(10 * 1024 * 1024):
                    await outfh.write(chunk)
    return out_path


async def load_json_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def load_csv_rows(path: Path) -> AsyncGenerator[Dict[str, str], None]:
    async with aiofiles.open(path, "r", encoding="utf8") as fh:
        async for row in AsyncDictReader(fh):
            yield row
    return
