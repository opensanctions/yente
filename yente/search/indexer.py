import asyncio
import threading
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from elasticsearch.exceptions import BadRequestError
from followthemoney import model

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.manifest import StatementManifest
from yente.data import refresh_manifest, get_datasets, get_manifest
from yente.search.base import get_es, close_es
from yente.search.mapping import make_entity_mapping, make_statement_mapping
from yente.search.mapping import INDEX_SETTINGS
from yente.data.util import expand_dates

log = get_logger(__name__)


async def entity_docs(dataset: Dataset, index: str):
    idx = 0
    async for entity in dataset.entities():
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
        idx += 1

        texts = entity.pop("indexText")
        data = entity.to_dict()
        data["canonical_id"] = entity.id
        data["text"] = texts
        data["dates"] = expand_dates(data.get("dates", []))
        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}

        for referent in entity.referents:
            if referent == entity.id:
                continue
            body = {"canonical_id": entity.id}
            yield {"_index": index, "_id": referent, "_source": body}


async def statement_docs(manifest: StatementManifest, index: str):
    idx = 0
    async for stmt in manifest.load():
        if idx % 10000 == 0 and idx > 0:
            log.info("Index: %d statements..." % idx, index=index)
        yield stmt.to_doc(index)
        idx += 1


def make_version(version: Optional[str]) -> str:
    full_version = settings.INDEX_VERSION
    if version is not None:
        full_version = f"{full_version}{version}"
    return full_version


@asynccontextmanager
async def versioned_index(
    es: AsyncElasticsearch,
    alias: str,
    dataset: str,
    version: str,
    mapping: Dict[str, Any],
    force: bool = False,
):
    try:
        dataset_prefix = f"{alias}-{dataset}"
        next_index = f"{dataset_prefix}-{version}"
        exists = await es.indices.exists(index=next_index)
        if exists.body and not force:
            log.info("Index is up to date.", index=next_index)
            yield None
            return

        # await es.indices.delete(index=next_index)
        log.info("Create index", index=next_index)
        try:
            await es.indices.create(
                index=next_index,
                mappings=mapping,
                settings=INDEX_SETTINGS,
            )
        except BadRequestError as exc:
            log.warning("Cannot create index: %s" % exc.message, index=next_index)

        try:
            yield next_index
        except (KeyboardInterrupt, OSError, Exception) as exc:
            log.exception("Indexing error: %s" % exc)
            await es.indices.delete(index=next_index)
            return

        await es.indices.refresh(index=next_index)
        res = await es.indices.put_alias(index=next_index, name=alias)
        if res.meta.status != 200:
            log.error("Failed to alias next index", index=next_index)
            return

        log.info("Index is now aliased to: %s" % alias, index=next_index)
        indices = await es.cat.indices(format="json")
        current: List[str] = [s.get("index") for s in indices]
        current = [c for c in current if c.startswith(f"{dataset_prefix}-")]
        if len(current) == 0:
            log.error("No index was created", index=next_index)
            return
        for index in current:
            if index != next_index:
                log.info("Delete other index", index=index)
                await es.indices.delete(index=index)
    finally:
        await es.close()


async def index_entities(dataset: Dataset, force: bool):
    """Index entities in a particular dataset, with versioning of the index."""
    es = await get_es()
    # Versioning defaults to the software version instead of a data update date:
    version = make_version(dataset.version)
    log.info(
        "Indexing entities",
        name=dataset.name,
        url=dataset.manifest.url,
        version=version,
    )
    schemata = list(model.schemata.values())
    mapping = make_entity_mapping(schemata)
    async with versioned_index(
        es,
        settings.ENTITY_INDEX,
        dataset.name,
        version,
        mapping,
        force=force,
    ) as next_index:
        if next_index is not None:
            docs = entity_docs(dataset, next_index)
            await async_bulk(
                es,
                docs,
                yield_ok=False,
                stats_only=True,
                chunk_size=1000,
                refresh="false",
            )


async def index_statements(manifest: StatementManifest, force: bool):
    if not settings.STATEMENT_API:
        log.warning("Statement API is disabled, not indexing statements.")
        return

    version = make_version(manifest.version)
    log.info(
        "Indexing statements",
        name=manifest.name,
        url=manifest.url,
        version=version,
    )
    es = await get_es()
    mapping = make_statement_mapping()
    async with versioned_index(
        es,
        settings.STATEMENT_INDEX,
        manifest.name,
        version,
        mapping,
        force=force,
    ) as next_index:
        if next_index is not None:
            docs = statement_docs(manifest, next_index)
            await async_bulk(
                es,
                docs,
                yield_ok=False,
                stats_only=True,
                chunk_size=2000,
                refresh=False,
            )


async def update_index(force: bool = False) -> None:
    try:
        await refresh_manifest()
        manifest = await get_manifest()
        datasets = await get_datasets()
        log.info("Index update check")
        indexers = []
        for dataset in datasets.values():
            if dataset.is_loadable:
                indexers.append(index_entities(dataset, force))
        for stmt in manifest.statements:
            indexers.append(index_statements(stmt, force))
        await asyncio.gather(*indexers)
        log.info("Index update complete.")
    finally:
        await close_es()


def update_index_threaded(force: bool = False) -> None:
    async def update_in_thread():
        await update_index(force=force)

    thread = threading.Thread(
        target=asyncio.run,
        args=(update_in_thread(),),
        daemon=True,
    )
    thread.start()
    # asyncio.to_thread(update_index, force=force)
