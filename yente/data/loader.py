import yaml
import httpx
import orjson
import asyncio
import aiofiles
from pathlib import Path
from itertools import count
from typing import Any, AsyncGenerator

from yente import settings
from yente.logs import get_logger
from yente.data.util import get_url_local_path, httpx_session

log = get_logger(__name__)


async def load_yaml_url(url: str) -> Any:
    if url.lower().endswith(".json"):
        return await load_json_url(url)
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "r") as fh:
            data = await fh.read()
    else:
        async with httpx_session() as client:
            resp = await client.get(url)
            data = resp.text
    return yaml.safe_load(data)


async def load_json_url(url: str) -> Any:
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "rb") as fh:
            data = await fh.read()
    else:
        async with httpx_session() as client:
            resp = await client.get(url)
            data = resp.content
    return orjson.loads(data)


async def fetch_url_to_path(url: str, path: Path) -> None:
    async with httpx_session() as client:
        async with client.stream('GET', url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(path, "wb") as outfh:
                async for chunk in resp.aiter_bytes():
                    await outfh.write(chunk)


async def read_path_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def stream_http_lines(url: str) -> AsyncGenerator[Any, None]:
    for retry in count():
        try:
            async with httpx_session() as client:
                async with client.stream('GET', url) as resp:
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        yield orjson.loads(line)
                    return
        except httpx.TransportError as exc:
            if retry > 3:
                raise
            await asyncio.sleep(1.0)
            log.error("Streaming index HTTP error: %s, retrying..." % exc)

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
        