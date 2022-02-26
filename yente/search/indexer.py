import structlog
import asyncstdlib as a
from typing import Iterable
from datetime import datetime
from structlog.stdlib import BoundLogger
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from followthemoney import model
from followthemoney.schema import Schema

from yente import settings
from yente.entity import Dataset
from yente.data import check_update, get_dataset_entities, get_statements, get_scope
from yente.search.base import get_es
from yente.search.mapping import make_entity_mapping, make_statement_mapping
from yente.search.mapping import INDEX_SETTINGS

log: BoundLogger = structlog.get_logger(__name__)


async def entity_docs(dataset: Dataset, index: str):
    entities = get_dataset_entities(dataset)
    async for idx, entity in a.enumerate(entities):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
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
    async for idx, row in a.enumerate(get_statements()):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d statements..." % idx, index=index)
        stmt_id = row.pop("id")
        yield {"_index": index, "_id": stmt_id, "_source": row}


def versioned_index(base_index: str, timestamp: datetime):
    ts = timestamp.strftime("%Y%m%d%H%M%S")
    return f"{base_index}-{ts}"


async def deploy_versioned_index(
    es: AsyncElasticsearch,
    base_alias: str,
    next_index: str,
):
    await es.indices.refresh(index=next_index)
    await es.indices.forcemerge(index=next_index)

    await es.indices.put_alias(index=next_index, name=base_alias)
    log.info("Index is now aliased to: %s" % base_alias, index=next_index)

    indices = await es.cat.indices(format="json")
    for spec in indices:
        name = spec.get("index")
        if name.startswith(f"{base_alias}-") and name != next_index:
            log.info("Delete existing index: %s" % name, index=name)
            await es.indices.delete(index=name)


async def index_entities(
    dataset: Dataset,
    schemata: Iterable[Schema],
    timestamp: datetime,
):
    next_index = versioned_index(settings.ENTITY_INDEX, timestamp)
    es = await get_es()
    exists = await es.indices.exists(index=next_index)
    if exists.body:
        log.info("Index is up to date.", index=next_index)
        # await es.indices.delete(index=next_index)
        return

    mapping = make_entity_mapping(schemata)
    log.info("Create index", index=next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)
    docs = entity_docs(dataset, next_index)
    await async_bulk(es, docs, stats_only=True, chunk_size=1000, max_retries=5)
    await deploy_versioned_index(es, settings.ENTITY_INDEX, next_index)


async def index_statements(
    timestamp: datetime,
):
    if not settings.STATEMENT_API:
        log.warning("Statement API is disabled, not indexing statements.")
        return

    next_index = versioned_index(settings.STATEMENT_INDEX, timestamp)
    es = await get_es()
    exists = await es.indices.exists(index=next_index)
    if exists:
        log.info("Index is up to date.", index=next_index)
        # await es.indices.delete(index=next_index)
        return

    mapping = make_statement_mapping()
    log.info("Create index", index=next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)

    docs = statement_docs(next_index)
    await async_bulk(es, docs, stats_only=True, chunk_size=2000, max_retries=5)
    await deploy_versioned_index(es, settings.STATEMENT_INDEX, next_index)


async def update_index(force=False):
    await check_update()
    scope = await get_scope()
    schemata = list(model)
    timestamp = scope.last_export
    if force:
        timestamp = datetime.utcnow()
    log.info("Index update check", next_ts=timestamp)
    await index_entities(scope, schemata, timestamp)
    await index_statements(timestamp)
    log.info("Update check done")
