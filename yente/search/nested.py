import asyncio
from typing import Any, Dict, Iterable, List, Set, Tuple, Union, Optional

from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import (
    AdjacentResultsResponse,
    EntityAdjacentResponse,
    EntityResponse,
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


def initial_outbound_ids(entity: Entity, prop: Optional[Property] = None) -> Set[str]:
    if prop is None:
        return set(entity.get_type_values(registry.entity))
    elif prop.schema.edge:
        return set()
    return set(entity.get(prop))


def make_outbound_query(next_entities: Iterable[str]) -> Dict[str, Any] | None:
    if not next_entities:
        return None
    return {"ids": {"values": list(next_entities)}}


def make_inbound_query(
    inbound_ids: List[str], prop: Optional[Property] = None
) -> Dict[str, Any] | None:
    if not inbound_ids:
        return None
    if prop is None or prop.reverse is None:
        return {"terms": {"entities": inbound_ids}}
    else:
        return {
            "bool": {
                "must": [
                    {"terms": {f"properties.{prop.reverse.name}": inbound_ids}},
                    {"terms": {"schema": [prop.reverse.schema.name]}},
                ]
            }
        }


async def get_nested_entity(
    provider: SearchProvider,
    root: Entity,
    prop: Optional[Property] = None,
    sort: List[Any] = [],
    limit: int = settings.MAX_RESULTS,
    offset: int = 0,
) -> Tuple[EntityResponse, TotalSpec]:
    """
    Fetches adjacent entities up to one edge away from the root, nested within
    the provided entity.

    When prop is provided, only that property is considered for nesting.

    When pagination options are supplied, adjacent ids beyond the specified page are dropped.

    Also returns the number of directly-adjacent entities available.
    """
    inverted: Inverted = {}
    inbound_ids = [root.id]
    outbound_ids: Set[str] = initial_outbound_ids(root, prop)
    entities: Entities = {root.id: root}
    total = None

    # The first iteration is outbound references from the root, and/or
    # inbound references from interstitial entities to the root,
    # and must be paginated. The second iteration is outbound references from
    # interstitial entities and must not be paginated. We assume that interstitial
    # entities are always connecting one entity to close to one other entities
    # to avoid getting into the notion of paginating a second step in a graph.
    while True:
        shoulds = []
        inbound_query = make_inbound_query(inbound_ids, prop)
        if inbound_query:
            shoulds.append(inbound_query)
        outbound_query = make_outbound_query(outbound_ids)
        if outbound_query:
            shoulds.append(outbound_query)

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
            size=limit,
            from_=offset,
            sort=sort,
        )

        if total is None:
            total = result_total(resp)
        # Prepare for second iteration
        limit = settings.MAX_RESULTS
        offset = 0
        inbound_ids = []
        outbound_ids.clear()

        for adj in result_entities(resp):
            if adj.id is None:
                continue
            entities[adj.id] = adj

            for adj_prop, value in adj.itervalues():
                if adj_prop.type != registry.entity:
                    continue
                if adj.schema.edge and value not in entities:
                    outbound_ids.add(value)

                inverted.setdefault(value, set())
                if adj_prop.reverse is not None:
                    inverted[value].add((adj_prop.reverse, adj.id))

    truncate_ids = bool(limit) or bool(offset)
    nested = nest_entity(root, entities, inverted, set(), truncate_ids)
    assert total is not None  # we expect to have had at least one iteration which sets total.
    return nested, total


async def get_adjacent_entities(
    provider: SearchProvider, entity: Entity, limit: int, offset: int, sort: List[Any]
) -> EntityAdjacentResponse:
    """Queries the requested page of results for each property of type Entity."""
    tasks = []
    async with asyncio.TaskGroup() as tg:
        for prop_name, prop in entity.schema.properties.items():
            if prop.type != registry.entity:
                continue
            task = tg.create_task(
                get_nested_entity(provider, entity, prop, sort, limit, offset)
            )
            tasks.append((prop_name, task))
    responses = {}
    for prop_name, task in tasks:
        prop_response, total = task.result()
        if total.value:
            responses[prop_name] = AdjacentResultsResponse(
                results=prop_response.properties.get(prop_name, []),
                total=total,
            )
    return EntityAdjacentResponse(
        entity=EntityResponse.from_entity(entity),
        adjacent=responses,
        limit=limit,
        offset=offset,
    )
