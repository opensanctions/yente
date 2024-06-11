import asyncio
import threading
from typing import Any, AsyncGenerator, Dict, List
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch.exceptions import BadRequestError
from followthemoney import model
from followthemoney.exc import FollowTheMoneyException
from followthemoney.types.date import DateType
from httpx import HTTPStatusError

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset, get_delta_version
from yente.data import get_catalog
from yente.data.loader import load_json_lines, load_json_url
from yente.search.base import (
    get_es,
    close_es,
    index_lock,
    get_current_version,
    SearchProvider,
    Index,
)
from yente.search.mapping import make_entity_mapping
from yente.search.mapping import INDEX_SETTINGS
from yente.search.mapping import NAMES_FIELD, NAME_PHONETIC_FIELD
from yente.search.mapping import NAME_PART_FIELD, NAME_KEY_FIELD
from yente.data.util import expand_dates, phonetic_names
from yente.data.util import index_name_parts, index_name_keys

log = get_logger(__name__)


async def iter_entity_docs(
    dataset: Dataset, index: str
) -> AsyncGenerator[Dict[str, Any], None]:
    if dataset.entities_url is None:
        return
    datasets = set(dataset.dataset_names)
    idx = 0
    async for data in load_json_lines(dataset.entities_url, index):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
        idx += 1

        try:
            entity = Entity.from_dict(model, data)
            entity.datasets = entity.datasets.intersection(datasets)
            if not len(entity.datasets):
                entity.datasets.add(dataset.name)
            if dataset.ns is not None:
                entity = dataset.ns.apply(entity)

            texts = entity.pop("indexText")
            doc = entity.to_full_dict(matchable=True)
            names: List[str] = doc.get(NAMES_FIELD, [])
            names.extend(entity.get("weakAlias", quiet=True))
            name_parts = index_name_parts(names)
            texts.extend(name_parts)
            doc[NAME_PART_FIELD] = name_parts
            doc[NAME_KEY_FIELD] = index_name_keys(names)
            doc[NAME_PHONETIC_FIELD] = phonetic_names(names)
            doc[DateType.group] = expand_dates(doc.pop(DateType.group, []))
            doc["text"] = texts

            entity_id = doc.pop("id")
            yield {"_index": index, "_id": entity_id, "_source": doc}
        except FollowTheMoneyException as exc:
            log.warning("Invalid entity: %s" % exc, data=data)


async def index_entities_rate_limit(
    es: AsyncElasticsearch, dataset: Dataset, force: bool
) -> bool:
    if index_lock.locked():
        log.info("Index is already being updated", dataset=dataset.name, force=force)
        return False
    with index_lock:
        return await index_entities(es, dataset, force=force)


async def index_entities(es: AsyncElasticsearch, dataset: Dataset, force: bool) -> bool:
    """Index entities in a particular dataset, with versioning of the index."""
    if not dataset.load:
        log.debug("Dataset is going to be loaded", dataset=dataset.name)
        return False
    if dataset.entities_url is None:
        log.warning(
            "Cannot identify resource with FtM entities",
            dataset=dataset.name,
        )
        return False

    # Versioning defaults to the newest delta version, otherwise it uses the software version.
    if newest := await dataset.newest_version():
        version = newest
    else:
        version = f"{settings.INDEX_VERSION}{dataset.version}"
    log.info(
        "Indexing entities",
        dataset=dataset.name,
        url=dataset.entities_url,
        version=version,
    )
    dataset_prefix = f"{settings.ENTITY_INDEX}-{dataset.name}-"
    next_index = f"{dataset_prefix}{version}"
    if settings.INDEX_EXISTS_ABORT:
        exists = await es.indices.exists(index=next_index)
    else:
        exists = await es.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=next_index,
        )
    if exists.body and not force:
        log.info("Index is up to date.", index=next_index)
        return False

    # await es.indices.delete(index=next_index)
    log.info("Create index", index=next_index)
    try:
        schemata = list(model.schemata.values())
        mapping = make_entity_mapping(schemata)
        await es.indices.create(
            index=next_index,
            mappings=mapping,
            settings=INDEX_SETTINGS,
        )
    except BadRequestError as exc:
        log.warning(
            "Cannot create index: %s" % exc.message,
            index=next_index,
        )

    try:
        docs = iter_entity_docs(dataset, next_index)
        await async_bulk(es, docs, yield_ok=False, stats_only=True, chunk_size=1000)
    except (
        BulkIndexError,
        KeyboardInterrupt,
        OSError,
        Exception,
        asyncio.TimeoutError,
        asyncio.CancelledError,
    ) as exc:
        errors = None
        if isinstance(exc, BulkIndexError):
            errors = exc.errors
        log.exception(
            "Indexing error: %r" % exc,
            dataset=dataset.name,
            index=next_index,
            errors=errors,
            entities_url=dataset.entities_url,
        )
        is_aliased = await es.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=next_index,
        )
        if not is_aliased.body:
            log.warn("Deleting partial index", index=next_index)
            await es.indices.delete(index=next_index)
        return False

    await es.indices.refresh(index=next_index)
    res = await es.indices.put_alias(index=next_index, name=settings.ENTITY_INDEX)
    if res.meta.status != 200:
        log.error("Failed to alias next index", index=next_index)
        return False
    log.info("Index is now aliased to: %s" % settings.ENTITY_INDEX, index=next_index)

    res = await es.indices.get_alias(name=settings.ENTITY_INDEX)
    for aliased_index in res.body.keys():
        if aliased_index == next_index:
            continue
        if aliased_index.startswith(dataset_prefix):
            log.info("Delete old index", index=aliased_index)
            res = await es.indices.delete(index=aliased_index)
    return True


async def update_index(force: bool = False) -> bool:
    """Reindex all datasets if there is a new version of their data contenst available,
    return boolean to indicate if the index was changed for any of them."""
    es_ = await get_es()
    es = es_.options(request_timeout=300)
    try:
        catalog = await get_catalog()
        log.info("Index update check")
        changed = False
        for dataset in catalog.datasets:
            _changed = await index_entities_rate_limit(es, dataset, force)
            changed = changed or _changed
        log.info("Index update complete.", changed=changed)
        return changed
    finally:
        await es.close()
        await close_es()


def update_index_threaded(force: bool = False) -> None:
    async def update_in_thread() -> None:
        try:
            await update_index(force=force)
        except (Exception, KeyboardInterrupt) as exc:
            log.exception("Index update error: %s" % exc)

    thread = threading.Thread(
        target=asyncio.run,
        args=(update_in_thread(),),
        daemon=True,
    )
    thread.start()
    # asyncio.to_thread(update_index, force=force)


class DeltasNotAvailable(Exception):
    pass


async def get_deltas_from_version(
    version: str, dataset: Dataset
) -> AsyncGenerator[str, None]:
    """
    Get deltas from a specific version of a dataset.
    """
    version = version.replace(settings.INDEX_VERSION, "")
    try:
        async for line in load_json_lines(
            get_delta_version(dataset.name, version), "test"
        ):
            yield line
    except HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise DeltasNotAvailable(f"No deltas found for {version}")


async def get_delta_versions() -> AsyncGenerator[Dict[str, List], None]:
    catalog = await get_catalog()
    for dataset in catalog.datasets:
        if dataset.delta_index is not None:
            try:
                yield await load_json_url(dataset.delta_index)
            except HTTPStatusError as exc:
                log.exception(f"Failed to load deltas for {dataset.name}: {exc}")
                continue


async def get_next_version(dataset: Dataset, version: str) -> None:
    """
    Get the next version of a dataset if versions are available.
    Return None if the dataset is up to date.
    """
    if dataset.delta_index is None:
        raise DeltasNotAvailable(f"No delta_index path specified for {dataset.name}")

    available_versions = [
        settings.INDEX_VERSION + v for v in await dataset.available_versions()
    ]
    try:
        ix = available_versions.index(version)
    except ValueError:
        raise DeltasNotAvailable(
            f"Current version of dataset not found in available versions: {dataset.name}, {version}"
        )
    if ix == len(available_versions) - 1:
        log.info(
            "Dataset is up to date.",
            dataset=dataset.name,
            current_version=version,
        )
        return
    next_version = available_versions[ix + 1]
    return next_version


async def delta_update_index(force: bool = True):
    # Get the catalog of datasets
    catalog = await get_catalog()
    log.info("Index update check")
    provider = await SearchProvider.create()
    clone = None
    for dataset in catalog.datasets:
        try:
            current_version = await get_current_version(dataset, provider)
            index = Index(provider, dataset.name, current_version)
            target_version = await dataset.newest_version()
            # If delta versioning is not implemented, update the index from scratch.
            if target_version is None:
                raise DeltasNotAvailable(f"No versions available for {dataset.name}")
            if current_version == target_version:
                log.info(
                    "Dataset is up to date.",
                    dataset=dataset.name,
                    current_version=current_version,
                )
                continue
            clone = await index.clone(target_version)
            # Get the next version.
            while next_version := await get_next_version(dataset, current_version):
                # Get the deltas from the next version and pass them to the bulk update.
                await clone.bulk_update(get_deltas_from_version(next_version, dataset))
                current_version = next_version
            # Set the cloned index as the current index.
            await clone.make_main()
        except Exception as exc:
            log.exception(f"Error updating index for {dataset.name}: {exc}")
            if clone is not None:
                await clone.delete()
            _changed = await index_entities_rate_limit(provider.client, dataset, force)
