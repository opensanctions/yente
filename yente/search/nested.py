from typing import Dict, List, Set, Tuple, Union, Optional
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import EntityResponse
from yente.provider import SearchProvider
from yente.search.search import result_entities

log = get_logger(__name__)

Value = Union[str, EntityResponse]
Entities = Dict[str, Entity]
Inverted = Dict[str, Set[Tuple[Property, str]]]


def nest_entity(
    entity: Entity, entities: Entities, inverted: Inverted, path: Set[Optional[str]]
) -> EntityResponse:
    props: Dict[str, List[Value]] = {}
    next_path = set([entity.id]).union(path)

    # Find other entities pointing to the one we're processing:
    if entity.id is not None:
        for prop, adj_id in inverted.get(entity.id, {}):
            if adj_id in path or len(path) > 1:
                continue
            adj = entities.get(adj_id)
            if adj is not None:
                nested = nest_entity(adj, entities, inverted, next_path)
                props.setdefault(prop.name, [])
                props[prop.name].append(nested)

    # Expand nested entities:
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        values: List[Value] = []
        for value in entity.get(prop):
            if value in path:
                continue
            adj = entities.get(value)
            if adj is not None:
                nested = nest_entity(adj, entities, inverted, next_path)
                values.append(nested)
            else:
                values.append(value)
        props[prop.name] = values
        if not len(values):
            props.pop(prop.name)
    serialized = EntityResponse.from_entity(entity)
    serialized.properties.update(props)
    return serialized


async def serialize_entity(
    provider: SearchProvider, root: Entity, nested: bool = False
) -> EntityResponse:
    if not nested or root.id is None:
        return EntityResponse.from_entity(root)
    inverted: Inverted = {}
    reverse = [root.id]

    entities: Entities = {root.id: root}
    next_entities = set(root.get_type_values(registry.entity))

    while True:
        shoulds = []
        if len(reverse):
            shoulds.append({"terms": {"entities": reverse}})

        if len(next_entities):
            shoulds.append({"ids": {"values": list(next_entities)}})

        if not len(shoulds):
            break
        query = {
            "bool": {
                "should": shoulds,
                "minimum_should_match": 1,
                "must_not": [{"ids": {"values": list(entities.keys())}}],
            }
        }

        resp = await provider.search(
            index=settings.ENTITY_INDEX,
            query=query,
            size=settings.MAX_RESULTS,
        )
        reverse = []
        next_entities.clear()
        for adj in result_entities(resp):
            if adj.id is None:
                continue
            entities[adj.id] = adj

            for prop, value in adj.itervalues():
                if prop.type != registry.entity:
                    continue
                if adj.schema.edge and value not in entities:
                    next_entities.add(value)

                inverted.setdefault(value, set())
                if prop.reverse is not None:
                    inverted[value].add((prop.reverse, adj.id))

    return nest_entity(root, entities, inverted, set())
