import asyncio
import structlog
from structlog.stdlib import BoundLogger
from asyncstdlib.functools import cache
from nomenklatura.dataset import DataCatalog

from yente.data.manifest import Manifest
from yente.data.dataset import Dataset

log: BoundLogger = structlog.get_logger(__name__)
fetch_lock = asyncio.Lock()


@cache
async def fetch_catalog() -> DataCatalog[Dataset]:
    manifest = await Manifest.load()
    catalog = DataCatalog(Dataset, {})
    for dmf in manifest.datasets:
        catalog.make_dataset(dmf)
    return catalog


async def get_catalog() -> DataCatalog[Dataset]:
    async with fetch_lock:
        return await fetch_catalog()


async def refresh_catalog() -> None:
    log.info("Refreshing manifest/catalog...")
    fetch_catalog.cache_clear()
    await get_catalog()
