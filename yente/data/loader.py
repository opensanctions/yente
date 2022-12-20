import yaml
import orjson
import aiofiles
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, AsyncGenerator

from yente import settings
from yente.logs import get_logger
from yente.data.util import http_session, resolve_url_type

BUFFER = 10 * 1024 * 1024

log = get_logger(__name__)


async def load_yaml_url(url: str) -> Any:
    url_ = resolve_url_type(url)
    if isinstance(url_, Path):
        async with aiofiles.open(url_, "r") as fh:
            data = await fh.read()
    else:
        async with http_session() as client:
            async with client.get(url) as resp:
                data = await resp.text()
    return yaml.safe_load(data)


async def fetch_url_to_path(url: str, path: Path) -> None:
    async with http_session() as client:
        async with client.get(url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(path, "wb") as outfh:
                while chunk := await resp.content.read(BUFFER):
                    await outfh.write(chunk)


async def read_path_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def load_json_lines(url: str, base_name: str) -> AsyncGenerator[Any, None]:
    parsed = urlparse(url)
    if parsed.scheme.lower() == "file":
        if parsed.path is None:
            raise ValueError("Invalid path: %s" % url)
        path = Path(parsed.path).resolve()
        log.info("Reading local data", url=url, path=path.as_posix())
        async for line in read_path_lines(path):
            yield line

    elif not settings.STREAM_LOAD:
        path = settings.DATA_PATH.joinpath(base_name)
        log.info("Fetching data", url=url, path=path.as_posix())
        try:
            await fetch_url_to_path(url, path)
            async for line in read_path_lines(path):
                yield line
        finally:
            path.unlink(missing_ok=True)
    else:
        log.info("Streaming data", url=url)
        async with http_session() as client:
            async with client.get(url) as resp:
                resp.raise_for_status()
                async for line in resp.content:
                    yield orjson.loads(line)
