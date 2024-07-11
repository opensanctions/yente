import asyncio
import threading
from typing import Any, AsyncGenerator, Dict, List
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch.exceptions import BadRequestError, NotFoundError
from followthemoney import model
from followthemoney.exc import FollowTheMoneyException
from followthemoney.types.date import DateType

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data import get_catalog
from yente.data.updater import DatasetUpdater
from yente.search.mapping import (
    make_entity_mapping,
    NAME_PART_FIELD,
    NAME_KEY_FIELD,
    INDEX_SETTINGS,
    NAMES_FIELD,
    NAME_PHONETIC_FIELD,
)
from yente.search.provider import SearchProvider, with_provider
from yente.search.util import parse_index_name
from yente.search.util import construct_index_name
from yente.data.util import expand_dates, phonetic_names
from yente.data.util import index_name_parts, index_name_keys


log = get_logger(__name__)
lock = threading.Lock()


async def iter_entity_docs(
    updater: DatasetUpdater, index: str
) -> AsyncGenerator[Dict[str, Any], None]:
    dataset = updater.dataset
    datasets = set(dataset.dataset_names)
    idx = 0
    ops: Dict[str, int] = {"ADD": 0, "DEL": 0, "MOD": 0}
    async for data in updater.load():
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
    provider: SearchProvider, alias: str, next_index: str, prefix: str
) -> None:
    """Remove all existing indices with a given prefix from the alias and
    add the new one."""
    actions = []
    actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
    actions.append({"add": {"index": next_index, "alias": alias}})
    await provider.client.indices.update_aliases(actions=actions)
    log.info("Index is now aliased to: %s" % settings.ENTITY_INDEX, index=next_index)


async def clone_index(provider: SearchProvider, base_version: str, target_version: str):
    """Create a copy of the index with the given name."""
    if base_version == target_version:
        raise ValueError("Cannot clone an index to itself.")
    try:
        await provider.client.indices.put_settings(
            index=base_version,
            settings={"index.blocks.read_only": True},
        )
        await provider.client.indices.delete(
            index=target_version, allow_no_indices=True
        )
        await provider.client.indices.clone(
            index=base_version,
            target=target_version,
            body={
                "settings": {"index": {"blocks": {"read_only": False}}},
            },
        )
        log.info("Cloned index", base=base_version, target=target_version)
    finally:
        await provider.client.indices.put_settings(
            index=base_version,
            settings={"index.blocks.read_only": False},
        )


async def create_index(provider: SearchProvider, index: str):
    """Create a new index with the given name."""
    log.info("Create index", index=index)
    try:
        schemata = list(model.schemata.values())
        mapping = make_entity_mapping(schemata)
        await provider.client.indices.create(
            index=index,
            mappings=mapping,
            settings=INDEX_SETTINGS,
        )
    except BadRequestError as exc:
        log.warning(
            "Cannot create index: %s" % exc.message,
            index=index,
        )


async def get_index_version(provider: SearchProvider, dataset: Dataset) -> str | None:
    """Return the currently indexed version of a given dataset."""
    try:
        resp = await provider.client.indices.get_alias(name=settings.ENTITY_INDEX)
    except NotFoundError:
        return None
    versions: List[str] = []
    for index in resp.keys():
        try:
            ds, version = parse_index_name(index)
            if ds == dataset.name:
                versions.append(version)
        except ValueError:
            pass
    if len(versions) == 0:
        return None
    # Return the oldest version of the index. If multiple versions are linked to the
    # alias, it's a sign that a previous index update failed. So we're erring on the
    # side of caution and returning the oldest version.
    return min(versions)


async def index_entities_rate_limit(
    provider: SearchProvider, dataset: Dataset, force: bool
) -> bool:
    if lock.locked():
        log.info("Index is already being updated", dataset=dataset.name, force=force)
        return False
    with lock:
        return await index_entities(provider, dataset, force=force)


async def index_entities(
    provider: SearchProvider, dataset: Dataset, force: bool
) -> bool:
    """Index entities in a particular dataset, with versioning of the index."""
    base_version = await get_index_version(provider, dataset)
    updater = await DatasetUpdater.build(dataset, base_version, force_full=force)
    if not updater.needs_update():
        return False
    log.info(
        "Indexing entities",
        dataset=dataset.name,
        url=dataset.entities_url,
        version=updater.target_version,
    )
    next_index = construct_index_name(dataset.name, updater.target_version)
    if settings.INDEX_EXISTS_ABORT:
        exists = await provider.client.indices.exists(index=next_index)
    else:
        exists = await provider.client.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=next_index,
        )
    if exists.body and not force:
        log.info("Index is up to date.", index=next_index)
        return False

    # await es.indices.delete(index=next_index)
    if updater.is_incremental and not force:
        base_index = construct_index_name(dataset.name, updater.base_version)
        await clone_index(provider, base_index, next_index)
    else:
        await create_index(provider, next_index)

    try:
        docs = iter_entity_docs(updater, next_index)
        await async_bulk(
            provider.client, docs, yield_ok=False, stats_only=True, chunk_size=1000
        )
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
        is_aliased = await provider.client.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=next_index,
        )
        if not is_aliased.body:
            log.warn("Deleting partial index", index=next_index)
            await provider.client.indices.delete(index=next_index)
        return False

    await provider.refresh(index=next_index)
    dataset_prefix = construct_index_name(dataset.name)
    # FIXME: we're not actually deleting old indexes here any more!
    await rollover_index(
        provider, settings.ENTITY_INDEX, next_index, prefix=dataset_prefix
    )
    return True


async def update_index(force: bool = False) -> bool:
    """Reindex all datasets if there is a new version of their data contenst available,
    return boolean to indicate if the index was changed for any of them."""
    async with with_provider() as provider:
        catalog = await get_catalog()
        log.info("Index update check")
        changed = False
        for dataset in catalog.datasets:
            _changed = await index_entities_rate_limit(provider, dataset, force)
            changed = changed or _changed
        log.info("Index update complete.", changed=changed)
        # TODO: what if we just deleted all indexes with the prefix but not linked to
        # the alias here?
        return changed


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
