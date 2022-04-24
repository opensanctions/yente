import structlog
from structlog.stdlib import BoundLogger
from typing import AsyncGenerator
from typing import Any, Dict, List, Tuple
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.data.entity import Entity
from yente.data.common import EntityResponse
from yente.search.base import get_es, get_opaque_id
from yente.search.search import result_entities

log: BoundLogger = structlog.get_logger(__name__)


async def get_adjacent(
    entity: Entity, exclude: List[str]
) -> AsyncGenerator[Tuple[Property, Entity], None]:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    entities = entity.get_type_values(registry.entity)
    entities = [e for e in entities if e not in exclude]
    # if len(entities):
    #     query = {"bool": {"filter": [{"ids": {"values": entities}}]}}
    #     size = min(len(entities), settings.MAX_RESULTS)
    #     print("ADJ QUERY", query)
    #     resp = await es_.search(index=settings.ENTITY_INDEX, query=query, size=size)
    #     for adj in result_entities(resp):
    #         for prop, value in entity.itervalues():
    #             if prop.type == registry.entity and value == adj.id:
    #                 yield prop, adj

    # Disable scoring by using a filter query
    shoulds = [{"term": {"entities": entity.id}}]
    if len(entities):
        shoulds.append({"ids": {"values": entities}})
    query = {
        "bool": {
            "should": shoulds,
            "minimum_should_match": 1,
            "must_not": [{"ids": {"values": exclude}}],
        }
    }
    resp = await es_.search(
        index=settings.ENTITY_INDEX,
        query=query,
        size=settings.MAX_RESULTS,
    )
    for adj in result_entities(resp):
        for prop, value in adj.itervalues():
            if prop.type == registry.entity and value == entity.id:
                if prop.reverse is not None:
                    yield prop.reverse, adj
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == adj.id:
                    yield prop, adj


async def _to_nested_dict(
    entity: Entity, depth: int, path: List[str]
) -> EntityResponse:
    next_depth = depth if entity.schema.edge else depth - 1
    next_path = path + [entity.id]
    resp = EntityResponse.from_entity(entity)
    if next_depth < 0:
        return resp
    nested: Dict[str, Any] = {}
    async for prop, adjacent in get_adjacent(entity, next_path):
        value = await _to_nested_dict(adjacent, next_depth, next_path)
        if prop.name not in nested:
            nested[prop.name] = []
        nested[prop.name].append(value)
    resp.properties.update(nested)
    return resp


async def serialize_entity(entity: Entity, nested: bool = False) -> EntityResponse:
    depth = 1 if nested else -1
    return await _to_nested_dict(entity, depth=depth, path=[])
