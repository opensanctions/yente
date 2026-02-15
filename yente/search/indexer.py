import asyncio
import threading
from typing import Any, AsyncGenerator, AsyncIterable, Dict, List, Set
from followthemoney.exc import FollowTheMoneyException
from followthemoney import registry

from yente import settings
from yente.data.manifest import Catalog
from yente.exc import YenteIndexError
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data import get_catalog
from yente.data.updater import DatasetUpdater
from yente.search.lock import (
    LockSession,
    acquire_lock,
    get_lock_index_name,
    refresh_lock,
    release_lock,
)
from yente.search import audit_log
from yente.search.audit_log import get_audit_log_index_name, AuditLogEventType
from yente.search.mapping import (
    NAME_PART_FIELD,
    NAME_PHONETIC_FIELD,
    NAME_SYMBOLS_FIELD,
    make_entity_mapping,
    INDEX_SETTINGS,
)
from yente.provider import SearchProvider, with_provider
from yente.search.versions import (
    build_index_name_prefix,
    parse_index_name,
    build_index_name,
    get_system_version,
)
from yente.data.util import expand_dates
from yente.data.util import index_symbol, is_matchable_symbol
from yente.data.util import entity_names


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

    name_parts: Set[str] = set()
    name_phonemes: Set[str] = set()
    name_symbols: Set[str] = set()
    for name in entity_names(entity):
        for symbol in name.symbols:
            if is_matchable_symbol(symbol):
                name_symbols.add(index_symbol(symbol))
        for part in name.parts:
            name_parts.add(part.form)
            name_parts.add(part.comparable)
            phoneme = part.metaphone
            if phoneme is not None and len(phoneme) > 2:
                name_phonemes.add(phoneme)

    doc[NAME_PART_FIELD] = list(name_parts)
    # doc[NAME_KEY_FIELD] = list(name_keys)
    doc[NAME_PHONETIC_FIELD] = list(name_phonemes)
    doc[NAME_SYMBOLS_FIELD] = list(name_symbols)
    if registry.date.group is not None:
        doc[registry.date.group] = expand_dates(doc.pop(registry.date.group, []))

    # TODO(Leon Handreke): Is name_parts needed here? All the fields get a copy_to text anyways in the mapper
    doc["text"] = entity.pop("indexText") + list(name_parts)

    return doc


async def get_index_version(provider: SearchProvider, dataset: Dataset) -> str | None:
    """Return the currently indexed version of a given dataset."""
    versions: List[str] = []
    for index in await provider.get_alias_indices(settings.ENTITY_INDEX):
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
    provider: SearchProvider, dataset: Dataset, force: bool, lock_session: LockSession
) -> None:
    """Index entities in a particular dataset, with versioning of the index."""
    alias = settings.ENTITY_INDEX
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

    await audit_log.log_audit_message(
        provider,
        AuditLogEventType.REINDEX_STARTED,
        index=next_index,
        dataset=dataset.name,
        dataset_version=updater.target_version,
        message=f"{'Incremental' if is_partial_reindex else 'Full'} reindex of {dataset.name} to {next_index} started",
    )

    if is_partial_reindex:
        assert (
            updater.base_version is not None
        ), "Expected base version to be set for partial reindex"
        base_index = build_index_name(dataset.name, updater.base_version)
        try:
            await provider.clone_index(base_index, next_index)
        except Exception as exc:
            log.error(
                "Clone failed",
                error=str(exc),
                dataset=dataset.name,
                base_index=base_index,
                target_index=next_index,
            )
            await provider.delete_index(next_index)
            raise
    else:
        # Note: this will only create the index if it doesn't already exist.
        # The implication of this is that the index is not re-created, even if --force is used.
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
                # lose the lock, expiration time is lock.LOCK_EXPIRATION_TIME (currently 10 minutes)
                if idx % 50000 == 0:
                    lock_refreshed = await refresh_lock(provider, lock_session)
                    if not lock_refreshed:
                        raise YenteIndexError(
                            "Failed to refresh re-index lock, aborting re-index"
                        )
                yield item

        await provider.bulk_index(refresh_lock_iterator(docs))
        await audit_log.log_audit_message(
            provider,
            AuditLogEventType.REINDEX_COMPLETED,
            index=next_index,
            dataset=dataset.name,
            dataset_version=updater.target_version,
            message=f"{'Incremental' if is_partial_reindex else 'Full'} reindex of {dataset.name} to {next_index} completed",
        )

    except Exception as exc:
        detail = getattr(exc, "detail", str(exc))
        log.exception(
            "Indexing error: %s" % detail,
            dataset=dataset.name,
            index=next_index,
        )
        await audit_log.log_audit_message(
            provider,
            AuditLogEventType.REINDEX_FAILED,
            index=next_index,
            dataset=dataset.name,
            dataset_version=updater.target_version,
            message=f"Failed to index entities to {next_index}: {detail}",
        )

        aliases = await provider.get_alias_indices(alias)
        if next_index not in aliases:
            log.warn("Deleting partial index", index=next_index)
            await provider.delete_index(next_index)
        if updater.is_incremental and not force:
            # This is tricky: try again with a full reindex if the incremental
            # indexing failed
            log.warn("Retrying with full reindex", dataset=dataset.name)
            return await index_entities(
                provider, dataset, force=True, lock_session=lock_session
            )
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
        AuditLogEventType.INDEX_ALIAS_ROLLOVER_COMPLETE,
        index=next_index,
        dataset=dataset.name,
        dataset_version=updater.target_version,
        message=f"Alias {alias} prefixed {dataset_prefix} now points to {next_index}",
    )
    log.info("Index is now aliased to: %s" % alias, index=next_index)


async def delete_old_indices(provider: SearchProvider, catalog: Catalog) -> None:
    aliased = await provider.get_alias_indices(settings.ENTITY_INDEX)
    for index in await provider.get_all_indices():
        if not index.startswith(settings.ENTITY_INDEX):
            continue
        # The lock and audit log indices live in the same namespace and shouldn't be garbage collected
        # TODO(Leon Handreke): They live in settings.INDEX_NAME, we should be safe actually. Remove this.
        if index in [get_audit_log_index_name(), get_lock_index_name()]:
            continue

        try:
            index_info = parse_index_name(index)
        except ValueError as exc:
            log.warn("Invalid index name: %s, deleting." % exc, index=index)
            await audit_log.log_audit_message(
                provider,
                AuditLogEventType.CLEANUP_INDEX_DELETED,
                index=index,
                message=f"Deleting index {index} due to invalid name",
            )
            await provider.delete_index(index)
            continue

        if index not in aliased:
            log.info("Deleting orphaned index", index=index)
            await audit_log.log_audit_message(
                provider,
                AuditLogEventType.CLEANUP_INDEX_DELETED,
                index=index,
                dataset=index_info.dataset_name,
                dataset_version=index_info.dataset_version,
                message=f"Deleting orphaned index {index}",
            )
            await provider.delete_index(index)
        dataset = catalog.get(index_info.dataset_name)
        if dataset is None or not dataset.model.load:
            log.info(
                "Deleting index of non-scope dataset",
                index=index,
                dataset=index_info.dataset_name,
            )
            await audit_log.log_audit_message(
                provider,
                AuditLogEventType.CLEANUP_INDEX_DELETED,
                index=index,
                dataset=index_info.dataset_name,
                dataset_version=index_info.dataset_version,
                message=f"Deleting index {index} due to non-scope dataset",
            )
            await provider.delete_index(index)


async def update_index(force: bool = False) -> None:
    """Reindex all datasets if there is a new version of their data contenst available,
    or create an initial version of the index from scratch."""
    async with with_provider() as provider:
        catalog = await get_catalog()
        log.info("Index update check")
        lock_session = await acquire_lock(provider)
        if not lock_session:
            log.warning("Failed to acquire lock, skipping index update")
            return
        try:
            for dataset in catalog.datasets:
                with lock:
                    await index_entities(
                        provider, dataset, force=force, lock_session=lock_session
                    )

            await delete_old_indices(provider, catalog)
            log.info("Index update complete.")
        finally:
            # It's important to release the lock after the index cleanup operations,
            # because the index cleanup can delete the index that another instance
            # is currently indexing to if not done in the locked section!
            await release_lock(provider, lock_session)


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
