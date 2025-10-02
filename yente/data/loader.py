import yaml
import httpx
import orjson
import asyncio
import aiofiles
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

from yente import settings
from yente.logs import get_logger
from yente.data.util import get_url_local_path, httpx_session

log = get_logger(__name__)

MAX_RETRIES = 3


async def load_yaml_url(url: str, auth_token: Optional[str] = None) -> Any:
    if url.lower().endswith(".json"):
        return await load_json_url(url, auth_token=auth_token)
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "r") as fh:
            data = await fh.read()
    else:
        async with httpx_session(auth_token=auth_token) as client:
            resp = await client.get(url)
            data = resp.text
    return yaml.safe_load(data)


async def load_json_url(url: str, auth_token: Optional[str] = None) -> Any:
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "rb") as fh:
            data = await fh.read()
    else:
        async with httpx_session(auth_token=auth_token) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
    return orjson.loads(data)


async def fetch_url_to_path(
    url: str, path: Path, auth_token: Optional[str] = None
) -> None:
    async with httpx_session(auth_token=auth_token) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(path, "wb") as outfh:
                async for chunk in resp.aiter_bytes():
                    await outfh.write(chunk)


async def read_path_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


async def stream_http_lines(
    url: str, auth_token: Optional[str] = None
) -> AsyncGenerator[Any, None]:
    async with httpx_session(auth_token=auth_token) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                yield orjson.loads(line)
            return


async def load_json_lines(
    url: str, base_name: str, auth_token: Optional[str] = None
) -> AsyncGenerator[Any, None]:
    path = get_url_local_path(url)
    if path is not None:
        log.info("Reading local data", url=url, path=path.as_posix())
        async for line in read_path_lines(path):
            yield line

    else:
        retries = 0
        if settings.STREAM_LOAD:
            log.info("Streaming data", url=url)
            try:
                async for line in stream_http_lines(url, auth_token=auth_token):
                    yield line
                # If we've managed to stream all the data, we're done
                return
            except httpx.HTTPError as e:
                log.error(
                    "Error streaming data, falling back to fetching instead",
                    url=url,
                    error=e,
                )
                retries += 1
                # Continue here by falling through to the fetch code
                # Note: this isn't really all that correct, the right way (tm) would be to bubble up
                # the error to the indexer and then do the right thing there (at least reset the counter).
                # But that's more work than I want to do right now and indexing is idempotent anyway.

        path = settings.DATA_PATH.joinpath(base_name)
        log.info("Fetching data", url=url, path=path.as_posix())

        while retries < MAX_RETRIES:
            try:
                await fetch_url_to_path(url, path, auth_token=auth_token)
                async for line in read_path_lines(path):
                    yield line
            except httpx.HTTPError as e:
                retries += 1
                log.error(
                    f"Error fetching data, this was attempt {retries}/{MAX_RETRIES}",
                    url=url,
                    error=e,
                )

                if retries >= MAX_RETRIES:
                    raise

                await asyncio.sleep(2**retries)
                continue

            finally:
                path.unlink(missing_ok=True)
