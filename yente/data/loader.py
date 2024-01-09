import yaml
import orjson
import aiofiles
from pathlib import Path
from typing import Any, AsyncGenerator

from yente import settings
from yente.logs import get_logger
from yente.data.util import http_session, get_url_local_path

log = get_logger(__name__)


async def load_yaml_url(url: str) -> Any:
    if url.lower().endswith(".json"):
        return await load_json_url(url)
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "r") as fh:
            data = await fh.read()
    else:
        async with http_session() as client:
            async with client.get(url) as resp:
                data = await resp.text()
    return yaml.safe_load(data)


async def load_json_url(url: str) -> Any:
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "rb") as fh:
            data = await fh.read()
    else:
        async with http_session() as client:
            async with client.get(url) as resp:
                data = await resp.read()
    return orjson.loads(data)


async def fetch_url_to_path(url: str, path: Path) -> None:
    async with http_session() as client:
        async with client.get(url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(path, "wb") as outfh:
                async for chunk, _ in resp.content.iter_chunks():
                    await outfh.write(chunk)


async def read_path_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def stream_http_lines(url: str) -> AsyncGenerator[Any, None]:
    async with http_session() as client:
        async with client.get(url, timeout=None) as resp:
            resp.raise_for_status()
            async for line in resp.content:
                yield orjson.loads(line)


async def load_json_lines(url: str, base_name: str) -> AsyncGenerator[Any, None]:
    path = get_url_local_path(url)
    if path is not None:
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
        async for line in stream_http_lines(url):
            yield line
        