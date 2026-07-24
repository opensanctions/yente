import yaml
import httpx
import orjson
import asyncio
import aiofiles
from hashlib import sha1
from pathlib import Path
from itertools import count
from typing import (
    Any,
)
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator

from yente import settings
from yente.exc import ChecksumError
from yente.logs import get_logger
from yente.data.util import get_url_local_path, httpx_session

log = get_logger(__name__)


def raise_for_status_with_custom_error(resp: httpx.Response) -> None:
    # Putting this custom error message here is a bit hacky, but the alternative
    # would have been to put it in the manifest and pass it through all the way here.
    # This is the pragmatic solution for now.
    if (
        resp.status_code == 401
        and resp.request.url.host == "delivery.opensanctions.com"
    ):
        raise httpx.HTTPStatusError(
            "Failed to authenticate to delivery.opensanctions.com with delivery token. See https://yente.followthemoney.tech/delivery/ for more information.",
            request=resp.request,
            response=resp,
        )

    resp.raise_for_status()


async def load_yaml_url(url: str, auth_token: str | None = None) -> Any:
    if url.lower().endswith(".json"):
        return await load_json_url(url, auth_token=auth_token)
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path) as fh:
            data = await fh.read()
    else:
        async with httpx_session(auth_token=auth_token) as client:
            resp = await client.get(url)
            data = resp.text
    return yaml.safe_load(data)


async def load_json_url(url: str, auth_token: str | None = None) -> Any:
    path = get_url_local_path(url)
    if path is not None:
        async with aiofiles.open(path, "rb") as fh:
            data = await fh.read()
    else:
        async with httpx_session(auth_token=auth_token) as client:
            resp = await client.get(url)
            # We want to provide a custom error message for unauthorized for delivery.opensanctions.com
            raise_for_status_with_custom_error(resp)
            data = resp.content
    return orjson.loads(data)


async def fetch_url_to_path(url: str, path: Path, auth_token: str | None = None) -> str:
    """Download url to path, returning the SHA1 hex digest of the downloaded bytes."""
    digest = sha1()
    async with httpx_session(auth_token=auth_token) as client:
        async with client.stream("GET", url) as resp:
            # We want to provide a custom error message for unauthorized for delivery.opensanctions.com
            raise_for_status_with_custom_error(resp)
            async with aiofiles.open(path, "wb") as outfh:
                async for chunk in resp.aiter_bytes():
                    digest.update(chunk)
                    await outfh.write(chunk)
    return digest.hexdigest()


async def read_path_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)


class HashingResponseStream:
    def __init__(self, stream: httpx.Response) -> None:
        self.stream = stream
        self.digest = sha1()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream.aiter_bytes():
            self.digest.update(chunk)
            yield chunk

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


async def split_json_lines(chunks: AsyncIterable[bytes]) -> AsyncGenerator[Any, None]:
    """Split byte chunks on newlines and yield each non-empty line as parsed JSON."""
    buf = b""
    async for chunk in chunks:
        buf += chunk
        while b"\n" in buf:
            raw_line, buf = buf.split(b"\n", 1)
            if raw_line.strip():
                yield orjson.loads(raw_line)
    if buf.strip():
        yield orjson.loads(buf)


class HttpJsonLinesStream:
    """Stream JSON lines from a URL; ``checksum`` holds the SHA1 hex digest once fully consumed."""

    def __init__(self, url: str, auth_token: str | None = None) -> None:
        self.url = url
        self.auth_token = auth_token
        self._checksum: str | None = None

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._stream()

    @property
    def checksum(self) -> str:
        if self._checksum is not None:
            return self._checksum

        msg = "HttpJsonLinesStream checksum is not available until iteration completes"
        raise RuntimeError(msg)

    async def _stream(self) -> AsyncGenerator[Any, None]:
        for retry in count():
            try:
                async with httpx_session(auth_token=self.auth_token) as client:
                    async with client.stream("GET", self.url) as resp:
                        # We want to provide a custom error message for unauthorized for delivery.opensanctions.com
                        raise_for_status_with_custom_error(resp)
                        hashed_stream = HashingResponseStream(resp)
                        async for line in split_json_lines(hashed_stream):
                            yield line
                self._checksum = hashed_stream.hexdigest()
                return
            except httpx.TransportError as exc:
                if retry > 3:
                    raise
                await asyncio.sleep(1.0)
                log.error(f"Streaming index HTTP error: {exc}, retrying...")


async def load_json_lines(
    url: str,
    base_name: str,
    auth_token: str | None = None,
    expected_checksum: str | None = None,
) -> AsyncGenerator[Any, None]:
    path = get_url_local_path(url)
    if path is not None:
        log.info("Reading local data", url=url, path=path.as_posix())
        async for line in read_path_lines(path):
            yield line
        return

    actual_checksum: str
    if not settings.STREAM_LOAD:
        path = settings.DATA_PATH.joinpath(base_name)
        log.info("Fetching data", url=url, path=path.as_posix())
        try:
            actual_checksum = await fetch_url_to_path(url, path, auth_token=auth_token)
            async for line in read_path_lines(path):
                yield line
        finally:
            path.unlink(missing_ok=True)
    else:
        log.info("Streaming data", url=url)
        stream = HttpJsonLinesStream(url, auth_token=auth_token)
        async for line in stream:
            yield line
        actual_checksum = stream.checksum

    if expected_checksum is not None and actual_checksum != expected_checksum:
        raise ChecksumError(actual=actual_checksum, expected=expected_checksum, url=url)
