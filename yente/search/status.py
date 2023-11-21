from yente import settings
from yente.logs import get_logger
from yente.search.base import get_es, close_es
from yente.data.manifest import Catalog

log = get_logger(__name__)


async def sync_dataset_versions(catalog: Catalog) -> None:
    es = await get_es()
    res = await es.indices.get_alias(name=settings.ENTITY_INDEX)
    for aliased_index in res.body.keys():
        aliased_end: str = aliased_index[len(settings.ENTITY_INDEX) + 1 :]
        dataset_name, index_version = aliased_end.split("-", 1)
        version = index_version[len(settings.INDEX_VERSION) :]
        dataset = catalog.get(dataset_name)
        if dataset is None:
            log.warn("Dataset has index but no metadata: %s" % dataset_name)
            continue
        if version != dataset.version:
            log.info(
                "Dataset %s is outdated" % dataset_name,
                indexed=version,
                available=dataset.version,
            )
        dataset.index_version = version
    await close_es()
