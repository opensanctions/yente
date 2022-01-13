import json
import asyncio
import logging
from typing import AsyncGenerator, List, Set
from httpx import AsyncClient
from datetime import datetime
from asyncstdlib.functools import cache
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property

from yente import settings
from yente.entity import Entity, Dataset, Datasets
from yente.models import FreebaseType
from yente.models import FreebaseEntity, FreebaseProperty

log = logging.getLogger(__name__)


@cache
async def get_data_index():
    async with AsyncClient() as client:
        response = await client.get(settings.DATA_INDEX)
        return response.json()


@cache
async def get_datasets() -> Datasets:
    index = await get_data_index()
    datasets: Datasets = {}
    for item in index.get("datasets", []):
        dataset = Dataset(item)
        datasets[dataset.name] = dataset
    return datasets


async def get_scope() -> Dataset:
    datasets = await get_datasets()
    dataset = datasets.get(settings.SCOPE_DATASET)
    if dataset is None:
        raise RuntimeError("Scope dataset does not exist: %s" % settings.SCOPE_DATASET)
    return dataset


async def get_export_time() -> datetime:
    scope = await get_scope()
    return scope.last_export


async def get_schemata() -> List[Schema]:
    schemata: List[Schema] = list()
    index = await get_data_index()
    for name in index.get("schemata"):
        schema = model.get(name)
        if schema is not None:
            schemata.append(schema)
    return schemata


async def get_matchable_schemata() -> Set[Schema]:
    schemata: Set[Schema] = set()
    for schema in await get_schemata():
        if schema.matchable:
            schemata.update(schema.schemata)
    return schemata


async def get_freebase_types() -> List[FreebaseType]:
    schemata = await get_matchable_schemata()
    return [get_freebase_type(s) for s in schemata]


def get_freebase_type(schema: Schema) -> FreebaseType:
    return {
        "id": schema.name,
        "name": schema.plural,
        "description": schema.description or schema.label,
    }


def get_freebase_entity(proxy: Entity, score: float = 0.0) -> FreebaseEntity:
    return {
        "id": proxy.id,
        "name": proxy.caption,
        "type": [get_freebase_type(proxy.schema)],
        "score": score,
        "match": False,
    }


def get_freebase_property(prop: Property) -> FreebaseProperty:
    return {
        "id": prop.qname,
        "name": prop.label,
        "description": prop.description,
    }


async def check_update():
    get_data_index.cache_clear()
    get_datasets.cache_clear()


async def get_dataset_entities(dataset: Dataset) -> AsyncGenerator[Entity, None]:
    if dataset.entities_url is None:
        raise ValueError("Dataset has no entity source: %s" % dataset)
    datasets = await get_datasets()
    async with AsyncClient() as client:
        async with client.stream("GET", dataset.entities_url) as response:
            async for line in response.aiter_lines():
                data = json.loads(line)
                entity = Entity.from_data(data, datasets)
                if not len(entity.datasets):
                    entity.datasets.add(dataset)
                yield entity


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_update())
