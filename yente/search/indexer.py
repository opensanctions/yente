import asyncio
import threading
from typing import Any, AsyncGenerator, Dict, List
from followthemoney import model
from followthemoney.exc import FollowTheMoneyException
from followthemoney.types.date import DateType

from yente import settings
from yente.data.manifest import Catalog
from yente.exc import YenteIndexError
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data import get_catalog
from yente.data.updater import DatasetUpdater
from yente.search.mapping import (
    NAME_PART_FIELD,
    NAME_KEY_FIELD,
    NAMES_FIELD,
    NAME_PHONETIC_FIELD,
)
from yente.provider import SearchProvider, with_provider
from yente.search.versions import parse_index_name
from yente.search.versions import construct_index_name
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


async def get_index_version(provider: SearchProvider, dataset: Dataset) -> str | None:
    """Return the currently indexed version of a given dataset."""
    versions: List[str] = []
    for index in await provider.get_alias_indices(settings.ENTITY_INDEX):
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


async def index_entities(
    provider: SearchProvider, dataset: Dataset, force: bool
) -> None:
    """Index entities in a particular dataset, with versioning of the index."""
    alias = settings.ENTITY_INDEX
    base_version = await get_index_version(provider, dataset)
    updater = await DatasetUpdater.build(dataset, base_version, force_full=force)
    if not updater.needs_update():
        if updater.dataset.load:
            log.info("No update needed", dataset=dataset.name, version=base_version)
        return
    log.info(
        "Indexing entities",
        dataset=dataset.name,
        url=dataset.entities_url,
        version=updater.target_version,
        base_version=updater.base_version,
        incremental=updater.is_incremental,
        # delta_urls=updater.delta_urls,
        force=force,
    )
    next_index = construct_index_name(dataset.name, updater.target_version)
    if not force and await provider.exists_index_alias(alias, next_index):
        log.info("Index is up to date.", index=next_index)
        return

    # await es.indices.delete(index=next_index)
    if updater.is_incremental and not force:
        base_index = construct_index_name(dataset.name, updater.base_version)
        await provider.clone_index(base_index, next_index)
    else:
        await provider.create_index(next_index)

    try:
        docs = iter_entity_docs(updater, next_index)
        await provider.bulk_index(docs)
    except (YenteIndexError, Exception) as exc:
        detail = getattr(exc, "detail", str(exc))
        log.exception(
            "Indexing error: %s" % detail,
            dataset=dataset.name,
            index=next_index,
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
    dataset_prefix = construct_index_name(dataset.name)
    # FIXME: we're not actually deleting old indexes here any more!
    await provider.rollover_index(
        alias,
        next_index,
        prefix=dataset_prefix,
    )
    log.info("Index is now aliased to: %s" % alias, index=next_index)


async def delete_old_indices(provider: SearchProvider, catalog: Catalog) -> None:
    aliased = await provider.get_alias_indices(settings.ENTITY_INDEX)
    for index in await provider.get_all_indices():
        if not index.startswith(settings.ENTITY_INDEX):
            continue
        if index not in aliased:
            log.info("Deleting orphaned index", index=index)
            await provider.delete_index(index)
        try:
            ds_name, _ = parse_index_name(index)
        except ValueError:
            continue
        dataset = catalog.get(ds_name)
        if dataset is None or not dataset.load:
            log.info(
                "Deleting index of non-scope dataset",
                index=index,
                dataset=ds_name,
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
