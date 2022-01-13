from typing import AsyncGenerator, Optional, Tuple
from nomenklatura.loader import Loader
from followthemoney.types import registry
from followthemoney.property import Property
from elasticsearch.exceptions import NotFoundError

from yente.settings import ES_INDEX
from yente.entity import Dataset, Datasets, Entity
from yente.search import result_entities, result_entity
from yente.search import filter_query, text_query
from yente.index import get_es
from yente.data import get_scope, get_datasets
from yente.util import EntityRedirect


async def get_loader():
    es = await get_es()
    dataset = await get_scope()
    datasets = await get_datasets()
    return IndexLoader(es, dataset, datasets)


class IndexLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    search index as a backend."""

    def __init__(self, es, dataset: Dataset, datasets: Datasets):
        self.es = es
        self.dataset = dataset
        self.datasets = datasets

    async def get_entity(self, id: str) -> Optional[Entity]:
        try:
            data = await self.es.get(index=ES_INDEX, id=id)
            _source = data.get("_source")
            if _source.get("canonical_id") != id:
                raise EntityRedirect(_source.get("canonical_id"))
            entity, _ = result_entity(self.datasets, data)
            return entity
        except NotFoundError:
            return None

    async def get_inverted(
        self, id: str
    ) -> AsyncGenerator[Tuple[Property, Entity], None]:
        # Do we need to query referents here?
        query = {"term": {"entities": id}}
        filtered = filter_query([query], self.dataset)
        resp = await self.es.search(index=ES_INDEX, query=filtered, size=9999)
        async for adj, _ in result_entities(resp):
            for prop, value in adj.itervalues():
                if prop.type == registry.entity and value == id:
                    if prop.reverse is not None:
                        yield prop.reverse, adj

    async def get_adjacent(
        self, entity: Entity, inverted: bool = True
    ) -> AsyncGenerator[Tuple[Property, Entity], None]:
        entities = entity.get_type_values(registry.entity)
        if len(entities):
            resp = await self.es.mget(index=ES_INDEX, body={"ids": entities})
            for raw in resp.get("docs", []):
                adj, _ = result_entity(self.datasets, raw)
                if adj is None:
                    continue
                for prop, value in entity.itervalues():
                    if prop.type == registry.entity and value == adj.id:
                        yield prop, adj

        if inverted:
            async for prop, inv in self.get_inverted(entity.id):
                if inv is not None:
                    yield prop, inv

    async def entities(self) -> AsyncGenerator[Entity, None]:
        if False:
            dummy = await self.get_entity("dummy")
            if dummy is not None:
                yield dummy
        raise NotImplemented

    async def count(self) -> int:
        q = text_query(self.dataset, None)
        response = await self.es.count(index=ES_INDEX, body=q)
        return response.get("count", 0)

    def __repr__(self):
        return f"<IndexLoader({self.dataset!r})>"
