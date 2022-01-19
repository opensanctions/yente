import asyncio
import logging
import aiocron
import asyncstdlib as a
from typing import Iterable
from datetime import datetime
from elasticsearch.helpers import async_bulk
from followthemoney import model
from followthemoney.schema import Schema

from yente.settings import ENTITY_INDEX, STATEMENT_INDEX
from yente.entity import Dataset
from yente.data import check_update, get_dataset_entities, get_statements, get_scope
from yente.index import get_es
from yente.mapping import make_entity_mapping, make_statement_mapping
from yente.mapping import INDEX_SETTINGS

log = logging.getLogger(__name__)


async def entity_docs(dataset: Dataset, index: str):
    entities = get_dataset_entities(dataset)
    async for idx, entity in a.enumerate(entities):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index [%s]: %d entities...", index, idx)
        texts = entity.pop("indexText")
        data = entity.to_dict()
        data["canonical_id"] = entity.id
        data["text"] = texts

        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}

        for referent in entity.referents:
            if referent == entity.id:
                continue
            body = {"canonical_id": entity.id}
            yield {"_index": index, "_id": referent, "_source": body}


async def statement_docs(index: str):
    # TMP:
    from hashlib import sha1

    def stmt_key(stmt):
        """Hash the key properties of a statement record to make a unique ID."""
        key = f"{stmt['dataset']}.{stmt['entity_id']}.{stmt['prop']}.{stmt['value']}"
        return sha1(key.encode("utf-8")).hexdigest()

    async for idx, row in a.enumerate(get_statements()):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index [%s]: %d statements...", index, idx)
        stmt_id = row.pop("id", stmt_key(row))
        yield {"_index": index, "_id": stmt_id, "_source": row}


def versioned_index(base_index: str, timestamp: datetime):
    ts = timestamp.strftime("%Y%m%d%H%M%S")
    return f"{base_index}-{ts}"


async def deploy_versioned_index(es, base_alias, next_index):
    await es.indices.refresh(index=next_index)
    await es.indices.forcemerge(index=next_index)

    await es.indices.put_alias(index=next_index, name=base_alias)
    log.info("Index [%s] is now aliased to: %s", next_index, base_alias)

    indices = await es.cat.indices(format="json")
    for spec in indices:
        name = spec.get("index")
        if name.startswith(f"{base_alias}-") and name != next_index:
            log.info("Delete existing index: %s", name)
            await es.indices.delete(index=name)


async def index_entities(
    dataset: Dataset,
    schemata: Iterable[Schema],
    timestamp: datetime,
):
    next_index = versioned_index(ENTITY_INDEX, timestamp)
    es = await get_es()
    exists = await es.indices.exists(index=next_index)
    if exists:
        log.info("Index [%s] is up to date.", next_index)
        # await es.indices.delete(index=next_index)
        return

    mapping = make_entity_mapping(schemata)
    log.info("Create index: %s", next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)
    docs = entity_docs(dataset, next_index)
    await async_bulk(es, docs, stats_only=True, chunk_size=2000)
    await deploy_versioned_index(es, ENTITY_INDEX, next_index)


async def index_statements(
    timestamp: datetime,
):
    next_index = versioned_index(STATEMENT_INDEX, timestamp)
    es = await get_es()
    exists = await es.indices.exists(index=next_index)
    if exists:
        log.info("Index [%s] is up to date.", next_index)
        # await es.indices.delete(index=next_index)
        return

    mapping = make_statement_mapping()
    log.info("Create index: %s", next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)

    docs = statement_docs(next_index)
    await async_bulk(es, docs, stats_only=True)
    await deploy_versioned_index(es, ENTITY_INDEX, next_index)


async def update_index(force=False):
    scope = await get_scope()
    schemata = list(model)
    timestamp = datetime.utcnow() if force else scope.last_export
    await asyncio.gather(
        index_entities(scope, schemata, timestamp),
        index_statements(timestamp),
    )


@aiocron.crontab("23 * * * *")
async def regular_update():
    await check_update()
    await update_index()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_index(force=True))
