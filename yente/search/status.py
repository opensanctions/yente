from yente import settings
from yente.logs import get_logger
from yente.provider import SearchProvider
from yente.search.versions import parse_index_name
from yente.data.manifest import Catalog

log = get_logger(__name__)


async def sync_dataset_versions(provider: SearchProvider, catalog: Catalog) -> None:
    for aliased_index in await provider.get_alias_indices(settings.ENTITY_INDEX):
        try:
            dataset_name, version = parse_index_name(aliased_index)
        except ValueError:
            log.warn("Invalid index name: %s" % aliased_index)
            continue
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
