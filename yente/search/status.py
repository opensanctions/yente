from yente import settings
from yente.logs import get_logger
from yente.provider import SearchProvider
from yente.search.versions import parse_index_name
from yente.data.manifest import Catalog

log = get_logger(__name__)


async def sync_dataset_versions(provider: SearchProvider, catalog: Catalog) -> None:
    for aliased_index in await provider.get_alias_indices(settings.ENTITY_INDEX):
        try:
            index_info = parse_index_name(aliased_index)
        except ValueError:
            log.warn("Invalid index name: %s" % aliased_index)
            continue
        dataset = catalog.get(index_info.dataset_name)
        if dataset is None:
            log.warn("Dataset has index but no metadata: %s" % index_info.dataset_name)
            continue
        if index_info.dataset_version != dataset.model.version:
            log.info(
                "Dataset %s is outdated" % index_info.dataset_name,
                indexed=index_info.dataset_version,
                available=dataset.model.version,
            )
        dataset.model.index_version = index_info.dataset_version
