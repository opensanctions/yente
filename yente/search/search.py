import logging
from elastic_transport import ObjectApiResponse
from structlog.contextvars import get_contextvars
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, cast
from elasticsearch import TransportError
from elasticsearch.exceptions import NotFoundError
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.entity import Entity
from yente.models import TotalSpec
from yente.search.base import get_es
from yente.search.queries import filter_query
from yente.data import get_datasets
from yente.util import EntityRedirect

log = logging.getLogger(__name__)


def get_opaque_id() -> str:
    ctx = get_contextvars()
    return ctx.get("trace_id")


def result_entity(datasets, data) -> Tuple[Optional[Entity], float]:
    source = data.get("_source")
    if source is None:
        return None, 0.0
    source["id"] = data.get("_id")
    return Entity.from_data(source, datasets), data.get("_score")


def result_total(result: ObjectApiResponse) -> TotalSpec:
    return cast(TotalSpec, result.get("hits", {}).get("total"))


async def result_entities(
    result: ObjectApiResponse,
) -> AsyncGenerator[Tuple[Entity, float], None]:
    datasets = await get_datasets()
    hits = result.get("hits", {})
    for hit in hits.get("hits", []):
        entity, score = result_entity(datasets, hit)
        if entity is not None:
            yield entity, score


async def search_entities(
    query: Dict[Any, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: Optional[Dict] = None,
) -> ObjectApiResponse:
    # pprint(query)
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    resp = await es_.search(
        index=settings.ENTITY_INDEX,
        query=query,
        size=limit,
        from_=offset,
        aggregations=aggregations,
    )
    return resp


async def query_results(
    query: Dict[Any, Any],
    limit: int,
    nested: bool = False,
    offset: int = 0,
    aggregations: Optional[Dict] = None,
):
    resp = await search_entities(
        query,
        limit=limit,
        offset=offset,
        aggregations=aggregations,
    )
    results = []
    async for result, score in result_entities(resp):
        data = await serialize_entity(result, nested=nested)
        data["score"] = score
        results.append(data)
    datasets = await get_datasets()
    facets = {}
    for field, agg in resp.get("aggregations", {}).items():
        facets[field] = {"label": field, "values": []}
        # print(field, agg)
        for bucket in agg.get("buckets", []):
            key = bucket.get("key")
            value = {"name": key, "label": key, "count": bucket.get("doc_count")}
            if field == "datasets":
                facets[field]["label"] = "Data sources"
                value["label"] = datasets[key].title
            if field in registry.groups:
                type_ = registry.groups[field]
                facets[field]["label"] = type_.plural
                value["label"] = type_.caption(key)
            facets[field]["values"].append(value)
    return {
        "results": results,
        "facets": facets,
        "total": result_total(resp),
        "limit": limit,
        "offset": offset,
    }


async def statement_results(
    query: Dict[str, Any], limit: int, offset: int
) -> Dict[str, Any]:
    es = await get_es()
    results = []
    es_ = es.options(opaque_id=get_opaque_id())
    resp = await es_.search(
        index=settings.STATEMENT_INDEX,
        query=query,
        size=limit,
        from_=offset,
    )
    # count_body = None if "match_all" in query else query
    # count_await = es.count(body=count_body, index=STATEMENT_INDEX)
    # resp, totals = await asyncio.gather(search_await, count_await)

    hits = resp.get("hits", {})
    for hit in hits.get("hits", []):
        source = hit.get("_source")
        source["id"] = hit.get("_id")
        results.append(source)
    return {
        "results": results,
        "total": result_total(resp),
        "limit": limit,
        "offset": offset,
    }


async def get_entity(entity_id: str) -> Optional[Entity]:
    es = await get_es()
    datasets = await get_datasets()
    try:
        es_ = es.options(opaque_id=get_opaque_id())
        data = await es_.get(index=settings.ENTITY_INDEX, id=entity_id)
        _source = data.get("_source")
        if _source.get("canonical_id") != entity_id:
            raise EntityRedirect(_source.get("canonical_id"))
        entity, _ = result_entity(datasets, data)
        return entity
    except NotFoundError:
        return None


async def get_adjacent(entity: Entity) -> AsyncGenerator[Tuple[Property, Entity], None]:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    entities = entity.get_type_values(registry.entity)
    datasets = await get_datasets()
    if len(entities):
        resp = await es_.mget(index=settings.ENTITY_INDEX, ids=entities)
        for raw in resp.get("docs", []):
            adj, _ = result_entity(datasets, raw)
            if adj is None:
                continue
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == adj.id:
                    yield prop, adj

    # Do we need to query referents here?
    query = {"term": {"entities": entity.id}}
    filtered = filter_query([query])

    resp = await es_.search(
        index=settings.ENTITY_INDEX,
        query=filtered,
        size=settings.MAX_PAGE,
    )
    async for adj, _ in result_entities(resp):
        for prop, value in adj.itervalues():
            if prop.type == registry.entity and value == entity.id:
                if prop.reverse is not None:
                    yield prop.reverse, adj


async def _to_nested_dict(
    entity: Entity, depth: int, path: List[str]
) -> Dict[str, Any]:
    next_depth = depth if entity.schema.edge else depth - 1
    next_path = path + [entity.id]
    data = entity.to_dict()
    if next_depth < 0:
        return data
    nested: Dict[str, Any] = {}
    async for prop, adjacent in get_adjacent(entity):
        if adjacent.id in next_path:
            continue
        value = await _to_nested_dict(adjacent, next_depth, next_path)
        if prop.name not in nested:
            nested[prop.name] = []
        nested[prop.name].append(value)
    data["properties"].update(nested)
    return data


async def serialize_entity(entity: Entity, nested: bool = False) -> Dict[str, Any]:
    depth = 1 if nested else -1
    return await _to_nested_dict(entity, depth=depth, path=[])


async def get_index_status() -> bool:
    es = await get_es()
    try:
        es_ = es.options(request_timeout=10, opaque_id=get_opaque_id())
        health = await es_.cluster.health()
        return health.get("status") in ("yellow", "green")
    except TransportError:
        return False
