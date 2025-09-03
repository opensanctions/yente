import asyncio
import threading
from typing import Any, AsyncGenerator, AsyncIterable, Dict, List
from followthemoney import registry
from followthemoney.exc import FollowTheMoneyException
from followthemoney.types.date import DateType

from yente.data.manifest import Catalog
from yente.exc import YenteIndexError
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data import get_catalog
from yente.data.updater import DatasetUpdater
from yente.search.audit_log import (
    acquire_reindex_lock,
    refresh_reindex_lock,
)
from yente.search import audit_log
from yente.search.audit_log import get_audit_log_index_name, AuditLogMessageType
from yente.search.mapping import (
    NAME_PART_FIELD,
    NAME_KEY_FIELD,
    NAME_PHONETIC_FIELD,
    NAME_SYMBOLS_FIELD,
    make_entity_mapping,
    INDEX_SETTINGS,
)
from yente.provider import SearchProvider, with_provider
from yente.search.versions import (
    get_index_alias_name,
    build_index_name_prefix,
    parse_index_name,
    build_index_name,
    get_system_version,
)
from yente.data.util import build_index_name_symbols, expand_dates, phonetic_names
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
            entity = Entity.from_dict(data["entity"])
            entity.datasets = entity.datasets.intersection(datasets)
            if not len(entity.datasets):
                entity.datasets.add(dataset.name)
            if dataset.ns is not None:
                entity = dataset.ns.apply(entity)

            yield {
                "_index": index,
                "_id": entity.id,
                "_source": build_indexable_entity_doc(entity),
            }
        except FollowTheMoneyException as exc:
            log.error("Invalid entity: %s" % exc, data=data)
    log.info(
        "Indexed %d entities" % idx,
        added=ops["ADD"],
        modified=ops["MOD"],
        deleted=ops["DEL"],
    )


def build_indexable_entity_doc(entity: Entity) -> Dict[str, Any]:
    doc = entity.to_dict(matchable=True)
    entity_id = doc.pop("id")
    doc["entity_id"] = entity_id

    # Total number of values in the entity, used to up-score on
    # large (i.e. important) entities.
    doc["entity_values_count"] = sum([len(v) for v in doc["properties"].values()])

    names: List[str] = entity.get_type_values(registry.name, matchable=True)
    names.extend(entity.get("weakAlias", quiet=True))

    name_parts = index_name_parts(entity.schema, names)
    doc[NAME_PART_FIELD] = list(name_parts)
    doc[NAME_KEY_FIELD] = list(index_name_keys(entity.schema, names))
    doc[NAME_PHONETIC_FIELD] = list(phonetic_names(entity.schema, names))
    if DateType.group is not None:
        doc[DateType.group] = expand_dates(doc.pop(DateType.group, []))

    name_symbols = build_index_name_symbols(entity)
    if name_symbols:
        doc[NAME_SYMBOLS_FIELD] = name_symbols

    # TODO(Leon Handreke): Is name_parts needed here? All the fields get a copy_to text anyways in the mapper
    doc["text"] = entity.pop("indexText") + list(name_parts)

    return doc


async def get_index_version(provider: SearchProvider, dataset: Dataset) -> str | None:
    """Return the currently indexed version of a given dataset."""
    versions: List[str] = []
    for index in await provider.get_alias_indices(get_index_alias_name()):
        try:
            index_info = parse_index_name(index)
            if index_info.system_version != get_system_version():
                log.debug("Skipping index with mismatched system version", index=index)
                continue
            if index_info.dataset_name == dataset.name:
                versions.append(index_info.dataset_version)
        except ValueError:
            log.warning("Skipping index with invalid name", index=index)
    if len(versions) == 0:
        return None
    # Return the oldest version of the index. If multiple versions are linked to the
    # alias, it's a sign that a previous index update failed. So we're erring on the
    # side of caution and returning the oldest version.
    return min(versions)


async def index_entities(
    provider: SearchProvider, dataset: Dataset, force: bool
) -> None:
    """Index entities in a particular dataset, with versioning of the index."""
    alias = get_index_alias_name()
    base_version = await get_index_version(provider, dataset)
    updater = await DatasetUpdater.build(dataset, base_version, force_full=force)
    if not updater.needs_update():
        if updater.dataset.model.load:
            log.info("No update needed", dataset=dataset.name, version=base_version)
        return
    log.info(
        "Indexing entities",
        dataset=dataset.name,
        url=dataset.model.entities_url,
        version=updater.target_version,
        base_version=updater.base_version,
        incremental=updater.is_incremental,
        # delta_urls=updater.delta_urls,
        force=force,
    )
    next_index = build_index_name(dataset.name, updater.target_version)
    if not force and await provider.exists_index_alias(alias, next_index):
        log.info("Index is up to date.", index=next_index)
        return

    is_partial_reindex = updater.is_incremental and not force
    audit_log_reindex_type = (
        audit_log.AuditLogReindexType.PARTIAL
        if is_partial_reindex
        else audit_log.AuditLogReindexType.FULL
    )
    # Acquire lock
    lock_acquired = await acquire_reindex_lock(
        provider,
        next_index,
        dataset=dataset.name,
        dataset_version=updater.target_version,
        reindex_type=audit_log_reindex_type,
    )
    if not lock_acquired:
        log.warning(
            "Failed to acquire lock, skipping index",
            dataset=dataset.name,
            index=next_index,
        )
        return

    if is_partial_reindex:
        assert (
            updater.base_version is not None
        ), "Expected base version to be set for partial reindex"
        base_index = build_index_name(dataset.name, updater.base_version)
        await provider.clone_index(base_index, next_index)
    else:
        await provider.create_index(
            next_index, mappings=make_entity_mapping(), settings=INDEX_SETTINGS
        )

    try:
        docs = iter_entity_docs(updater, next_index)

        # Little wrapper to refresh the lock every now and then
        async def refresh_lock_iterator(
            it: AsyncIterable[Dict[str, Any]],
        ) -> AsyncIterable[Dict[str, Any]]:
            idx = 0
            async for item in it:
                idx += 1
                # Refresh the lock every 50,000 documents. Should be enough not to
                # lose the lock, expiration time is lock.LOCK_EXPIRATION_TIME (currently 5 minutes)
                if idx % 50000 == 0:
                    lock_refreshed = await refresh_reindex_lock(provider, next_index)
                    if not lock_refreshed:
                        log.error(
                            f"Failed to refresh reindex lock for index {next_index}, continuing anyway"
                        )
                        raise YenteIndexError(
                            "Failed to refresh re-index lock, aborting re-index"
                        )
                yield item

        await provider.bulk_index(refresh_lock_iterator(docs))

        await audit_log.release_reindex_lock(
            provider,
            next_index,
            dataset=dataset.name,
            dataset_version=updater.target_version,
            reindex_type=audit_log_reindex_type,
            success=True,
        )

    except (YenteIndexError, Exception) as exc:
        detail = getattr(exc, "detail", str(exc))
        log.exception(
            "Indexing error: %s" % detail,
            dataset=dataset.name,
            index=next_index,
        )
        await audit_log.release_reindex_lock(
            provider,
            next_index,
            dataset=dataset.name,
            dataset_version=updater.target_version,
            reindex_type=audit_log_reindex_type,
            success=False,
        )

        aliases = await provider.get_alias_indices(alias)
        if next_index not in aliases:
            log.warn("Deleting partial index", index=next_index)
            await provider.delete_index(next_index)
        if updater.is_incremental and not force:
            # This is tricky: try again with a full reindex if the incremental
            # indexing failed
            log.warn("Retrying with full reindex", dataset=dataset.name)
            return await index_entities(provider, dataset, force=True)
        raise exc

    await provider.refresh(index=next_index)
    dataset_prefix = build_index_name_prefix(dataset.name)
    # FIXME: we're not actually deleting old indexes here any more!
    await provider.rollover_index(
        alias,
        next_index,
        prefix=dataset_prefix,
    )
    await audit_log.log_audit_message(
        provider,
        AuditLogMessageType.INDEX_ALIAS_ROLLOVER_COMPLETE,
        index=next_index,
        dataset=dataset.name,
        dataset_version=updater.target_version,
        reindex_type=audit_log_reindex_type,
    )
    log.info("Index is now aliased to: %s" % alias, index=next_index)


async def delete_old_indices(provider: SearchProvider, catalog: Catalog) -> None:
    aliased = await provider.get_alias_indices(get_index_alias_name())
    for index in await provider.get_all_indices():
        if not index.startswith(get_index_alias_name()):
            continue
        # The lock and audit log indices live in the same namespace and shouldn't be garbage collected
        if index in [get_audit_log_index_name()]:
            continue
        if index not in aliased:
            log.info("Deleting orphaned index", index=index)
            await provider.delete_index(index)
        try:
            index_info = parse_index_name(index)
        except ValueError as exc:
            log.warn("Invalid index name: %s, deleting." % exc, index=index)
            await provider.delete_index(index)
            continue
        dataset = catalog.get(index_info.dataset_name)
        if dataset is None or not dataset.model.load:
            log.info(
                "Deleting index of non-scope dataset",
                index=index,
                dataset=index_info.dataset_name,
            )
            await provider.delete_index(index)


async def update_index(force: bool = False) -> None:
    """Reindex all datasets if there is a new version of their data contenst available,
    or create an initial version of the index from scratch."""
    async with with_provider() as provider:
        catalog = await get_catalog()
        log.info("Index update check")
        for dataset in catalog.datasets:
            with lock:
                await index_entities(provider, dataset, force=force)

        await delete_old_indices(provider, catalog)
        log.info("Index update complete.")


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
