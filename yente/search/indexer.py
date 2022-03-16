import asyncio
import structlog
import threading
from typing import Any, Dict, Iterable, List
from datetime import datetime
from structlog.stdlib import BoundLogger
from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from elasticsearch.exceptions import BadRequestError
from followthemoney import model
from followthemoney.schema import Schema

from yente import settings
from yente.entity import Dataset
from yente.data import check_update, get_dataset_entities, get_statements, get_scope
from yente.search.base import get_es, close_es
from yente.search.mapping import make_entity_mapping, make_statement_mapping
from yente.search.mapping import INDEX_SETTINGS

log: BoundLogger = structlog.get_logger(__name__)


async def entity_docs(dataset: Dataset, index: str):
    idx = 0
    async for entity in get_dataset_entities(dataset):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
        idx += 1

        texts = entity.pop("indexText")
        data = entity.to_dict()
        data["canonical_id"] = entity.id
        data["text"] = texts
        # TODO: add partial dates

        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}

        for referent in entity.referents:
            if referent == entity.id:
                continue
            body = {"canonical_id": entity.id}
            yield {"_index": index, "_id": referent, "_source": body}


async def statement_docs(index: str):
    idx = 0
    async for row in get_statements():
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d statements..." % idx, index=index)
        stmt_id = row.pop("id")
        yield {"_index": index, "_id": stmt_id, "_source": row}
        idx += 1


@asynccontextmanager
async def versioned_index(
    es: AsyncElasticsearch,
    base_alias: str,
    mapping: Dict[str, Any],
    timestamp: datetime,
    force: bool = False,
):
    ts = timestamp.strftime("%Y%m%d%H%M%S")
    next_index = f"{base_alias}-{ts}"
    exists = await es.indices.exists(index=next_index)
    if exists.body and not force:
        log.info("Index is up to date.", index=next_index)
        # await es.indices.delete(index=next_index)
        yield None
        return

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
        await es.indices.refresh(index=next_index)
        await es.indices.put_alias(index=next_index, name=base_alias)
        log.info("Index is now aliased to: %s" % base_alias, index=next_index)
        indices = await es.cat.indices(format="json")
        current: List[str] = []
        for spec in indices:
            name = spec.get("index")
            if name.startswith(f"{base_alias}-"):
                current.append(name)

        if len(current) > 1:
            next_index = max(current)
            for index in current:
                if index != next_index:
                    log.info("Delete existing index: %s" % name, index=index)
                    await es.indices.delete(index=index)
            await es.indices.put_alias(index=next_index, name=base_alias)
    except (
        Exception,
        KeyboardInterrupt,
        RuntimeError,
        SystemExit,
    ) as exc:
        log.warning("Error [%r]; deleting partial index" % exc, index=next_index)
        await close_es()
        es = await get_es()
        await es.indices.delete(index=next_index, ignore_unavailable=True)
        raise


async def index_entities(
    dataset: Dataset,
    schemata: Iterable[Schema],
    timestamp: datetime,
    force: bool,
):
    es = await get_es()
    mapping = make_entity_mapping(schemata)
    async with versioned_index(
        es,
        settings.ENTITY_INDEX,
        mapping,
        timestamp,
        force=force,
    ) as next_index:
        if next_index is not None:
            docs = entity_docs(dataset, next_index)
            await async_bulk(
                es,
                docs,
                yield_ok=False,
                stats_only=True,
                chunk_size=500,
                max_retries=5,
                refresh=False,
            )


async def index_statements(timestamp: datetime, force: bool):
    if not settings.STATEMENT_API:
        log.warning("Statement API is disabled, not indexing statements.")
        return

    es = await get_es()
    mapping = make_statement_mapping()
    async with versioned_index(
        es,
        settings.STATEMENT_INDEX,
        mapping,
        timestamp,
        force=force,
    ) as next_index:
        if next_index is not None:
            docs = statement_docs(next_index)
            await async_bulk(
                es,
                docs,
                stats_only=True,
                chunk_size=2000,
                max_retries=5,
                refresh=False,
            )


async def update_index(force=False):
    await check_update()
    scope = await get_scope()
    schemata = list(model)
    log.info("Index update check", next_ts=scope.last_export)
    await index_entities(scope, schemata, scope.last_export, force)
    await index_statements(scope.last_export, force)
    log.info("Index update complete.", next_ts=scope.last_export)


def update_index_threaded(force=False):
    async def update_in_thread():
        await update_index(force=force)
        await close_es()

    thread = threading.Thread(
        target=asyncio.run,
        args=(update_in_thread(),),
        daemon=True,
    )
    thread.start()
    # asyncio.to_thread(update_index, force=force)
