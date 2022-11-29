import structlog
from structlog.stdlib import BoundLogger
from asyncstdlib.functools import cache
from nomenklatura.dataset import DataCatalog

from yente.data.manifest import Manifest
from yente.data.dataset import Dataset

log: BoundLogger = structlog.get_logger(__name__)


async def get_manifest() -> Manifest:
    return await Manifest.load()


@cache
async def get_catalog() -> DataCatalog[Dataset]:
    manifest = await Manifest.load()
    catalog = DataCatalog(Dataset, {})
    for dmf in manifest.datasets:
        catalog.make_dataset(dmf)
    return catalog


async def refresh_manifest() -> None:
    log.info("Refreshing manifest metadata...")
    get_catalog.cache_clear()
