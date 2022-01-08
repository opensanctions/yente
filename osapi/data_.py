import asyncio
from typing import List, Set
from httpx import AsyncClient
from datetime import datetime
from asyncstdlib.functools import cache
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property

from osapi import settings
from osapi.entity import Entity, Dataset, Datasets
from osapi.models import FreebaseType
from osapi.models import FreebaseEntity, FreebaseProperty


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
    # index = await get_data_index()
    # print(repr(await get_export_time()))
    print(await get_freebase_types())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_update())
