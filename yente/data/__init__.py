import asyncio
import structlog
from structlog.stdlib import BoundLogger

from yente.data.manifest import Catalog

log: BoundLogger = structlog.get_logger(__name__)
lock = asyncio.Lock()


async def get_catalog() -> Catalog:
    async with lock:
        if Catalog.instance is None:
            Catalog.instance = await Catalog.load()
    return Catalog.instance


async def refresh_catalog() -> None:
    log.info("Refreshing manifest/catalog...")
    Catalog.instance = await Catalog.load()
