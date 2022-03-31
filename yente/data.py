import json
import asyncio
import logging
from banal import as_bool
from aiohttp import ClientSession, ClientTimeout
from aiocsv import AsyncDictReader
from typing import Any, AsyncGenerator, Dict, List, Set
from asyncstdlib.functools import cache
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property

from yente import settings
from yente.entity import Entity, Dataset, Datasets
from yente.models import FreebaseType, FreebaseProperty
from yente.models import FreebaseEntity, FreebaseScoredEntity
from yente.util import AsyncTextReaderWrapper, iso_datetime

log = logging.getLogger(__name__)
http_timeout = ClientTimeout(
    total=3600 * 6,
    connect=None,
    sock_read=None,
    sock_connect=None,
)


@cache
async def get_data_index():
    async with ClientSession(timeout=http_timeout) as client:
        async with client.get(settings.DATA_INDEX) as resp:
            return await resp.json()


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


def get_freebase_entity(proxy: Entity) -> FreebaseEntity:
    return {
        "id": proxy.id,
        "name": proxy.caption,
        "type": [get_freebase_type(proxy.schema)],
    }


def get_freebase_scored(data: Dict[str, Any]) -> FreebaseScoredEntity:
    schema = model.get(data["schema"])
    return {
        "id": data["id"],
        "name": data["caption"],
        "type": [get_freebase_type(schema)],
        "score": int(data["score"] * 100),
        "match": data["match"],
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
    async with ClientSession(timeout=http_timeout, read_bufsize=2**17) as client:
        async with client.get(dataset.entities_url) as resp:
            async for line in resp.content:
                data = json.loads(line)
                entity = Entity.from_os_data(data, datasets)
                if not len(entity.datasets):
                    entity.datasets.add(dataset)
                yield entity


async def get_statements() -> AsyncGenerator[Dict[str, str], None]:
    index = await get_data_index()
    url = index.get("statements_url")
    if url is None:
        raise ValueError("No statement URL in index")
    async with ClientSession(timeout=http_timeout, read_bufsize=2**17) as client:
        async with client.get(url) as resp:
            wrapper = AsyncTextReaderWrapper(resp.content, "utf-8")
            async for row in AsyncDictReader(wrapper):
                row["target"] = as_bool(row["target"])
                row["unique"] = as_bool(row["unique"])
                row["first_seen"] = iso_datetime(row["first_seen"])
                row["last_seen"] = iso_datetime(row["last_seen"])
                yield row


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(test_get_statements())
