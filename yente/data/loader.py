import yaml
import orjson
import aiofiles
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, AsyncGenerator, Tuple, Union

from yente import settings
from yente.logs import get_logger
from yente.data.util import http_session, resolve_url_type

BUFFER = 10 * 1024 * 1024

log = get_logger(__name__)


async def get_url_path(url: str, base_name: str) -> Path:
    parsed = urlparse(url)
    if parsed.scheme.lower() == "file":
        if parsed.path is None:
            raise ValueError("Invalid path: %s" % url)
        return Path(parsed.path).resolve()
    out_path = settings.DATA_PATH.joinpath(base_name)
    async with http_session() as client:
        log.info("Fetching data", url=url, path=out_path.as_posix())
        async with client.get(url) as resp:
            async with aiofiles.open(out_path, "wb") as outfh:
                while chunk := await resp.content.read(BUFFER):
                    await outfh.write(chunk)
    return out_path


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


async def load_json_lines(path: Path) -> AsyncGenerator[Any, None]:
    async with aiofiles.open(path, "rb") as fh:
        async for line in fh:
            yield orjson.loads(line)
