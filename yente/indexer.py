import asyncio
import logging
import aiocron
import asyncstdlib as a
from typing import Iterable
from datetime import datetime
from elasticsearch.helpers import async_bulk
from followthemoney import model
from followthemoney.schema import Schema

from yente.settings import ES_INDEX
from yente.entity import Dataset
from yente.data import check_update, get_dataset_entities, get_scope
from yente.index import get_es
from yente.mapping import make_mapping, INDEX_SETTINGS

log = logging.getLogger(__name__)
# SKIP_ADJACENT = (registry.date, registry.entity, registry.topic)


async def entity_docs(dataset: Dataset, index: str):
    entities = get_dataset_entities(dataset)
    async for idx, entity in a.enumerate(entities):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index [%s]: %d entities...", index, idx)
        texts = entity.pop("indexText")
        data = entity.to_dict()
        data["canonical_id"] = entity.id
        data["text"] = texts
        # if entity.schema.is_a(BASE_SCHEMA):
        #     async for _, adj in loader.get_adjacent(entity):
        #         for prop, value in adj.itervalues():
        #             if prop.type in SKIP_ADJACENT:
        #                 continue
        #             field = prop.type.group or "text"
        #             if field not in data:
        #                 data[field] = []
        #             data[field].append(value)

        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}

        for referent in entity.referents:
            if referent == entity.id:
                continue
            body = {"canonical_id": entity.id}
            yield {"_index": index, "_id": referent, "_source": body}


async def index(dataset: Dataset, schemata: Iterable[Schema], timestamp: datetime):
    ts = timestamp.strftime("%Y%m%d%H%M%S")
    prefix = f"{ES_INDEX}-{dataset.name}"
    next_index = f"{prefix}-{ts}"
    es = await get_es()
    exists = await es.indices.exists(index=next_index)
    if exists:
        log.info("Index [%s] is up to date.", next_index)
        # await es.indices.delete(index=next_index)
        return

    mapping = make_mapping(schemata)
    log.info("Create index: %s", next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)
    docs = entity_docs(dataset, next_index)
    await async_bulk(es, docs, stats_only=True)
    log.info("Indexing done, force merge")
    await es.indices.refresh(index=next_index)
    await es.indices.forcemerge(index=next_index)

    log.info("Index [%s] is now aliased to: %s", next_index, ES_INDEX)
    await es.indices.put_alias(index=next_index, name=ES_INDEX)

    indices = await es.cat.indices(format="json")
    for spec in indices:
        name = spec.get("index")
        if name.startswith(prefix) and name != next_index:
            log.info("Delete existing index: %s", name)
            await es.indices.delete(index=name)


async def update_index(force=False):
    scope = await get_scope()
    schemata = list(model)
    timestamp = datetime.utcnow() if force else scope.last_export
    await index(scope, schemata, timestamp)


@aiocron.crontab("23 * * * *")
async def regular_update():
    await check_update()
    await update_index()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_index())
