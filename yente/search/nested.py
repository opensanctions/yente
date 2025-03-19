import asyncio
from typing import Any, Dict, List, Set, Tuple, Union, Optional
from pprint import pprint, pformat

from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import (
    AdjacentResultsResponse,
    EntityAdjacentResponse,
    EntityResponse,
    PropAdjacentResponse,
    TotalSpec,
)
from yente.provider import SearchProvider
from yente.search.search import result_entities, result_total

log = get_logger(__name__)

Value = Union[str, EntityResponse]
Entities = Dict[str, Entity]
Inverted = Dict[str, Set[Tuple[Property, str]]]


def nest_entity(
    entity: Entity,
    entities: Entities,
    inverted: Inverted,
    path: Set[Optional[str]],
    truncate_ids: bool = False,
) -> EntityResponse:
    """
    Args:
        inverted: A mapping of entity IDs to a set of tuples of
            (property, entity_id) where entity_id refers to the entity we're
            processing via property.
        path: Entity IDs already processed further up the tree.
            Prevents repetition in a path.
    """
    props: Dict[str, List[Value]] = {}
    next_path = set([entity.id]).union(path)

    # Find other entities pointing to the one we're processing:
    if entity.id is not None:
        for prop, adj_id in inverted.get(entity.id, {}):
            if adj_id in path or len(path) > 1:
                continue
            adj = entities.get(adj_id)
            if adj is not None:
                nested = nest_entity(adj, entities, inverted, next_path, truncate_ids)
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
                nested = nest_entity(adj, entities, inverted, next_path, truncate_ids)
                values.append(nested)
            else:
                if not truncate_ids:
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


async def get_adjacent_prop(
    provider: SearchProvider,
    root: Entity,
    query_prop: Property,
    limit: int,
    offset: int,
    sort: List[Any],
) -> PropAdjacentResponse:
    inverted: Inverted = {}
    reverse = [root.id]
    size = limit
    total = None
    query_offset = offset

    entities: Entities = {root.id: root}
    next_entities: Set[str] = set()
    if not query_prop.stub:
        next_entities.update(root.get(query_prop))

    while True:
        queries = []
        if len(reverse) and query_prop.reverse is not None:
            queries.append(
                {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    f"properties.{query_prop.reverse.name}": reverse
                                }
                            },
                            {"terms": {"schema": [query_prop.reverse.schema.name]}},
                        ]
                    }
                }
            )

        if len(next_entities):
            queries.append(
                {"bool": {"must": [{"ids": {"values": list(next_entities)}}]}}
            )

        if not len(queries):
            break
        query = {
            "bool": {
                "should": queries,
                "minimum_should_match": 1,
                "must_not": [{"ids": {"values": list(entities.keys())}}],
            }
        }
        resp = await provider.search(
            index=settings.ENTITY_INDEX,
            query=query,
            size=size,
            from_=query_offset,
            sort=sort,
        )

        # The first iteration is either outbound references from the root,
        # or inbound references from interstitial entities to the root,
        # and must be paginated.
        # The second iteration is outbound references from interstitial entities
        # and must not be paginated.
        #
        # Prepare for second iteration
        if total is None:
            total = result_total(resp)
        reverse = []
        size = settings.MAX_RESULTS
        query_offset = 0
        next_entities.clear()

        # Handle results
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

    nested = nest_entity(root, entities, inverted, set(), truncate_ids=True)
    results = nested.properties.get(query_prop.name, [])
    return PropAdjacentResponse(
        results=results,
        total=total,
        limit=limit,
        offset=offset,
    )


async def get_adjacents(
    provider: SearchProvider, entity: Entity, limit: int, offset: int, sort: List[Any]
) -> EntityAdjacentResponse:
    tasks = []
    async with asyncio.TaskGroup() as tg:
        for prop_name, prop in entity.schema.properties.items():
            if prop.type != registry.entity:
                continue
            task = tg.create_task(
                get_adjacent_prop(provider, entity, prop, limit, offset, sort)
            )
            tasks.append((prop_name, task))
    responses = {}
    for prop_name, task in tasks:
        prop_response = task.result()
        if prop_response.total.value:
            responses[prop_name] = AdjacentResultsResponse(
                results=prop_response.results,
                total=prop_response.total,
            )
    return EntityAdjacentResponse(
        entity=EntityResponse.from_entity(entity),
        adjacent=responses,
        limit=limit,
        offset=offset,
    )
