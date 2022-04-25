import structlog
from structlog.stdlib import BoundLogger
from typing import Optional, Dict, List, Tuple, Union
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.data.entity import Entity
from yente.data.common import EntityResponse
from yente.search.base import get_es, get_opaque_id
from yente.search.search import result_entities

log: BoundLogger = structlog.get_logger(__name__)

Entities = Dict[str, Optional[Entity]]
Inverted = Dict[str, List[Tuple[Property, str]]]


def nest_entity(
    entity: Entity,
    entities: Entities,
    inverted: Inverted,
    path: List[str] = [],
) -> EntityResponse:
    serialized = EntityResponse.from_entity(entity)
    props: Dict[str, List[Union[str, EntityResponse]]] = {}
    value: Union[str, EntityResponse] = ""
    next_path = path + [entity.id]
    for (prop, value) in inverted.get(entity.id, []):
        if prop.reverse is None or value in next_path:
            continue
        value_entity = entities.get(value)
        if value_entity is not None:
            value = nest_entity(
                value_entity,
                entities,
                inverted,
                path=next_path,
            )
        props.setdefault(prop.reverse.name, [])
        props[prop.reverse.name].append(value)
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        nested = []
        for value in entity.pop(prop):
            if value in next_path:
                continue
            value_entity = entities.get(value)
            if value_entity is not None:
                value = nest_entity(
                    value_entity,
                    entities,
                    inverted,
                    path=next_path,
                )
            nested.append(value)
        props[prop.name] = nested
    serialized.properties.update(props)
    return serialized


async def serialize_entity(root: Entity, nested: bool = False) -> EntityResponse:
    if not nested:
        return EntityResponse.from_entity(root)
    entities: Entities = {root.id: root}
    inverted: Inverted = {}
    reverse = [root.id]

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
        # from pprint import pprint
        # pprint(query)

        resp = await es_.search(
            index=settings.ENTITY_INDEX,
            query=query,
            size=settings.MAX_RESULTS,
        )

        reverse = []
        for adj in result_entities(resp):
            entities[adj.id] = adj

            # TODO: not sure this is needed:
            if adj.schema.edge:
                reverse.append(adj.id)

            for prop, value in adj.itervalues():
                if prop.type != registry.entity:
                    continue
                # if entities.get(value) is None:
                #     entities[value] = None
                if adj.schema.edge:
                    entities.setdefault(value, None)

                inverted.setdefault(value, [])
                inverted[value].append((prop, adj.id))

    for ent in entities.values():
        if ent.schema.name == "Sanction":
            print("SANC", ent)

    return nest_entity(root, entities, inverted)
