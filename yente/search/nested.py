import structlog
from structlog.stdlib import BoundLogger
from typing import Optional, Dict, List, Set, Tuple, Union
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.data.entity import Entity
from yente.data.common import EntityResponse
from yente.search.base import get_es, get_opaque_id
from yente.search.search import result_entities

log: BoundLogger = structlog.get_logger(__name__)

Value = Union[str, EntityResponse]
Entities = Dict[str, Optional[Entity]]
Inverted = Dict[str, Set[Tuple[Property, str]]]


def nest_value(
    entity_id: str, entities: Entities, inverted: Inverted, path: List[str]
) -> Value:
    entity = entities.get(entity_id)
    if entity_id in path or entity is None:
        return entity_id
    next_path = path + [entity.id]
    return nest_entity(entity, entities, inverted, next_path)


def nest_entity(
    entity: Entity, entities: Entities, inverted: Inverted, path: List[str]
) -> EntityResponse:
    serialized = EntityResponse.from_entity(entity)
    props: Dict[str, List[Value]] = {}
    for (prop, value) in inverted.get(entity.id, []):
        if value in path or len(path) > 1:
            continue
        invert = nest_value(value, entities, inverted, path)
        props.setdefault(prop.name, [])
        props[prop.name].append(invert)
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        nested: List[Value] = []
        for value in entity.pop(prop):
            nested.append(nest_value(value, entities, inverted, path))
        props[prop.name] = nested
    serialized.properties.update(props)
    return serialized


async def serialize_entity(root: Entity, nested: bool = False) -> EntityResponse:
    if not nested:
        return EntityResponse.from_entity(root)
    inverted: Inverted = {}
    reverse = [root.id]

    entities: Entities = {root.id: root}
    for forward_id in root.get_type_values(registry.entity):
        entities.setdefault(forward_id, None)

    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    while True:
        shoulds = []
        if len(reverse):
            shoulds.append({"terms": {"entities": reverse}})
        next_entities = [i for (i, e) in entities.items() if e is None]
        if len(next_entities):
            shoulds.append({"ids": {"values": next_entities}})
        if not len(shoulds):
            break
        seen_entities = [i for (i, e) in entities.items() if e is not None]
        query = {
            "bool": {
                "should": shoulds,
                "minimum_should_match": 1,
                "must_not": [{"ids": {"values": seen_entities}}],
            }
        }
        resp = await es_.search(
            index=settings.ENTITY_INDEX,
            query=query,
            size=settings.MAX_RESULTS,
        )

        reverse = []
        for adj in result_entities(resp):
            entities[adj.id] = adj

            # TODO: not sure this is needed:
            # if adj.schema.edge:
            #     reverse.append(adj.id)

            for prop, value in adj.itervalues():
                if prop.type != registry.entity:
                    continue
                if adj.schema.edge:
                    entities.setdefault(value, None)

                inverted.setdefault(value, set())
                if prop.reverse is not None:
                    inverted[value].add((prop.reverse, adj.id))

    return nest_entity(root, entities, inverted, [root.id])
