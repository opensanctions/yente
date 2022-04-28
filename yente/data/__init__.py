import structlog
from structlog.stdlib import BoundLogger
from asyncstdlib.functools import cache

from yente.data.manifest import Manifest
from yente.data.dataset import Dataset, Datasets

log: BoundLogger = structlog.get_logger(__name__)


@cache
async def get_manifest() -> Manifest:
    return await Manifest.load()


@cache
async def get_datasets() -> Datasets:
    manifest = await get_manifest()
    datasets: Datasets = {}
    for dmf in manifest.datasets:
        dataset = Dataset(datasets, dmf)
        datasets[dataset.name] = dataset
    return datasets


async def refresh_manifest():
    log.info("Refreshing manifest metadata...")
    get_manifest.cache_clear()
    get_datasets.cache_clear()
