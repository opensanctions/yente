from typing import Optional
from nomenklatura.loader import Loader

from osapi.entity import Dataset, Datasets, Entity


class IndexLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    search index as a backend."""

    def __init__(self, datasets: Datasets):
        self.datasets = datasets

    async def get_entity(self, id: str) -> Optional[Entity]:
        async for cached in self.db.query(self.dataset, entity_id=id):
            for entity in self.assemble(cached):
                return entity
        return None

    async def _get_inverted(self, id: str) -> AsyncGenerator[Entity, None]:
        async for cached in self.db.query(self.dataset, inverted_id=id):
            for entity in self.assemble(cached):
                yield entity

    async def get_inverted(
        self, id: str
    ) -> AsyncGenerator[Tuple[Property, Entity], None]:
        async for entity in self._get_inverted(id):
            for prop, value in entity.itervalues():
                if value == id and prop.reverse is not None:
                    yield prop.reverse, entity

    async def _iter_entities(self) -> AsyncGenerator[CachedEntity, None]:
        async for cached in self.db.query(self.dataset):
            yield cached

    async def entities(self) -> AsyncGenerator[Entity, None]:
        raise NotImplemented()

    async def count(self) -> int:
        async with engine.begin() as conn:
            return await count_entities(conn, self.dataset)

    def __repr__(self):
        return f"<IndexLoader({self.dataset!r})>"
