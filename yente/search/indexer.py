import asyncio
import threading
from typing import Any, AsyncGenerator, Dict, List, Set
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch.exceptions import BadRequestError, NotFoundError
from followthemoney import model
from followthemoney.exc import FollowTheMoneyException
from followthemoney.types.date import DateType
from httpx import HTTPStatusError

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data import get_catalog
from yente.data.loader import load_json_lines
from yente.data.delta import DatasetLoader
from yente.search.base import (
    get_es,
    close_es,
    index_lock,
    get_current_version,
    ESSearchProvider,
    SearchProvider,
    Index,
)
from yente.search.mapping import (
    make_entity_mapping,
    NAME_PART_FIELD,
    NAME_KEY_FIELD,
    INDEX_SETTINGS,
    NAMES_FIELD,
    NAME_PHONETIC_FIELD,
)
from yente.search.util import parse_index_name
from yente.search.util import construct_index_name, construct_index_version
from yente.data.util import expand_dates, phonetic_names
from yente.data.util import index_name_parts, index_name_keys


log = get_logger(__name__)


async def iter_entity_docs(
    loader: DatasetLoader, index: str, force: bool = False
) -> AsyncGenerator[Dict[str, Any], None]:
    dataset = loader.dataset
    datasets = set(dataset.dataset_names)
    idx = 0
    ops: Dict[str, int] = {"ADD": 0, "DEL": 0, "MOD": 0}
    async for data in loader.load(force_full=force):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
        op_code = data["op"]
        idx += 1
        ops[op_code] += 1
        if op_code == "DEL":
            yield {
                "_op_type": "delete",
                "_index": index,
                "_id": data["entity"]["id"],
            }
            continue

        try:
            entity = Entity.from_dict(model, data["entity"])
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
    log.info(
        "Indexed %d entities" % idx,
        added=ops["ADD"],
        modified=ops["MOD"],
        deleted=ops["DEL"],
    )


async def rollover_index(
    es: AsyncElasticsearch, alias: str, next_index: str, prefix: str
) -> None:
    """Remove all existing indices with a given prefix from the alias and
    add the new one."""
    actions = []
    actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
    actions.append({"add": {"index": next_index, "alias": alias}})
    await es.indices.update_aliases(actions=actions)
    log.info("Index is now aliased to: %s" % settings.ENTITY_INDEX, index=next_index)


async def clone_index(es: AsyncElasticsearch, base_version: str, target_version: str):
    """Create a copy of the index with the given name."""
    if base_version == target_version:
        raise ValueError("Cannot clone an index to itself.")
    try:
        await es.indices.put_settings(
            index=base_version,
            settings={"index.blocks.read_only": True},
        )
        await es.indices.delete(index=target_version, allow_no_indices=True)
        await es.indices.clone(
            index=base_version,
            target=target_version,
            body={
                "settings": {"index": {"blocks": {"read_only": False}}},
            },
        )
        log.info("Cloned index", base=base_version, target=target_version)
    finally:
        await es.indices.put_settings(
            index=base_version,
            settings={"index.blocks.read_only": False},
        )


async def create_index(es: AsyncElasticsearch, index: str):
    """Create a new index with the given name."""
    log.info("Create index", index=index)
    try:
        schemata = list(model.schemata.values())
        mapping = make_entity_mapping(schemata)
        await es.indices.create(
            index=index,
            mappings=mapping,
            settings=INDEX_SETTINGS,
        )
    except BadRequestError as exc:
        log.warning(
            "Cannot create index: %s" % exc.message,
            index=index,
        )


async def get_index_version(es: AsyncElasticsearch, dataset: Dataset) -> str | None:
    """Return the currently indexed version of a given dataset."""
    try:
        resp = await es.indices.get_alias(name=settings.ENTITY_INDEX)
    except NotFoundError:
        return None
    versions: List[str] = []
    for index in resp.keys():
        ds, version = parse_index_name(index)
        if ds == dataset.name:
            versions.append(version)
    if len(versions) == 0:
        return None
    # Return the oldest version of the index. If multiple versions are linked to the
    # alias, it's a sign that a previous index update failed. So we're erring on the
    # side of caution and returning the oldest version.
    return min(versions)


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
    base_version = await get_index_version(es, dataset)
    loader = await DatasetLoader.build(dataset, base_version)
    if not loader.check(force_full=force):
        return False
    log.info(
        "Indexing entities",
        dataset=dataset.name,
        url=dataset.entities_url,
        version=loader.target_version,
    )
    next_index = construct_index_name(dataset.name, loader.target_version)
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
    if loader.is_incremental and not force:
        base_index = construct_index_name(dataset.name, loader.base_version)
        await clone_index(es, base_index, next_index)
    else:
        await create_index(es, next_index)

    try:
        docs = iter_entity_docs(loader, next_index, force=force)
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
    dataset_prefix = construct_index_name(dataset.name)
    await rollover_index(es, settings.ENTITY_INDEX, next_index, prefix=dataset_prefix)
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
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Get deltas from a specific version of a dataset.
    """
    try:
        async for line in load_json_lines(dataset.delta_path(version), version):
            yield line
    except HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise DeltasNotAvailable(f"No deltas found for {version}")


async def get_next_version(dataset: Dataset, version: str) -> str | None:
    """
    Get the next version of a dataset if versions are available.
    Return None if the dataset is up to date.
    """

    available_versions = await dataset.available_versions()
    available_versions = sorted(available_versions)
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
        return None
    next_version = available_versions[ix + 1]
    return next_version


async def delta_update_index(dataset: Dataset, provider: SearchProvider) -> bool:
    if not dataset.load:
        log.debug("Dataset is not going to be loaded", dataset=dataset.name)
        return False
    clone = None
    try:
        current_version = await get_current_version(dataset, provider)
        if current_version is None:
            raise Exception("No index found for dataset.")
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
            return False
        clone = await index.clone(construct_index_version(target_version))
        # Get the next version.
        seen: Set[str] = set()
        while next_version := await get_next_version(dataset, current_version):
            log.info(
                f"Now updating {dataset.name} from version {current_version} to {next_version}"
            )
            seen.add(current_version)
            if next_version in seen:
                raise Exception(
                    f"Loop detected in versions for {dataset.name}: {next_version}"
                )
            # Get the deltas from the next version and pass them to the bulk update.
            await clone.bulk_update(get_deltas_from_version(next_version, dataset))
            current_version = next_version
        # Set the cloned index as the current index.
        await clone.make_main()
        return True
    except Exception as exc:
        log.info(
            f"Error updating index for {dataset.name}: {exc}\nStarting from scratch."
        )
        if clone is not None:
            await clone.delete()
        _changed = await index_entities(provider.client, dataset, True)
        return _changed


async def delta_update_catalog() -> None:
    # Get the catalog of datasets
    catalog = await get_catalog()
    log.info("Index update check")
    async with ESSearchProvider() as provider:
        for dataset in catalog.datasets:
            if index_lock.locked():
                log.info("Index is already being updated", dataset=dataset.name)
                continue
            with index_lock:
                await delta_update_index(dataset, provider)
