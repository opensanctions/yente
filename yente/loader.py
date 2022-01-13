from typing import AsyncGenerator, Optional, Tuple
from nomenklatura.loader import Loader
from followthemoney.types import registry
from followthemoney.property import Property
from elasticsearch.exceptions import NotFoundError

from yente.settings import ES_INDEX
from yente.entity import Dataset, Datasets, Entity
from yente.index import es


class IndexLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    search index as a backend."""

    def __init__(self, datasets: Datasets):
        self.datasets = datasets

    async def get_entity(self, id: str) -> Optional[Entity]:
        try:
            data = await es.get(index=ES_INDEX, id=id)
            entity, _ = result_entity(self.datasets, data)
            return entity
        except NotFoundError:
            return None

    async def _get_inverted(self, id: str) -> AsyncGenerator[Entity, None]:
        async for cached in self.db.query(self.dataset, inverted_id=id):
            for entity in self.assemble(cached):
                yield entity

    async def get_inverted(
        self, id: str
    ) -> AsyncGenerator[Tuple[Property, Entity], None]:
        # Do we need to query referents here?
        query = {"term": {"entities": id}}
        filtered = filter_query([query], dataset)
        resp = await es.search(index=ES_INDEX, query=filtered, size=9999)
        for adj, _ in result_entities(resp):
            for prop, value in adj.itervalues():
                if prop.type == registry.entity and value == id:
                    if prop.reverse is not None:
                        yield prop.reverse, adj

    async def get_adjacent(
        self, entity: Entity, inverted: bool = True
    ) -> AsyncGenerator[Tuple[Property, Entity], None]:

        # TODO mget

        if inverted:
            async for adj, prop in self.get_inverted(entity.id):
                yield adj, prop

    async def entities(self) -> AsyncGenerator[Entity, None]:
        if False:
            dummy = await self.get_entity("dummy")
            if dummy is not None:
                yield dummy
        raise NotImplemented

    async def count(self) -> int:
        # TODO add empty text query on dataset
        response = await es.count(index=ES_INDEX)
        return response.get("count", 0)

    def __repr__(self):
        return f"<IndexLoader({self.dataset!r})>"
