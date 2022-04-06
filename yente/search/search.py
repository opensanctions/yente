import structlog
from structlog.stdlib import BoundLogger
from structlog.contextvars import get_contextvars
from typing import AsyncGenerator, Generator, Union
from typing import Any, Dict, List, Optional, Tuple, cast
from elasticsearch import TransportError, ApiError
from elasticsearch.exceptions import NotFoundError
from elastic_transport import ObjectApiResponse
from followthemoney.property import Property
from followthemoney.types import registry

from yente import settings
from yente.entity import Datasets, Entity
from yente.models import TotalSpec
from yente.search.base import get_es
from yente.data import get_datasets
from yente.util import EntityRedirect

log: BoundLogger = structlog.get_logger(__name__)


def get_opaque_id() -> str:
    ctx = get_contextvars()
    return ctx.get("trace_id")


def result_entity(datasets, data) -> Optional[Entity]:
    source = data.get("_source")
    if source is None:
        return None
    source["id"] = data.get("_id")
    return Entity.from_os_data(source, datasets)


def result_total(result: ObjectApiResponse) -> TotalSpec:
    return cast(TotalSpec, result.get("hits", {}).get("total"))


def result_entities(
    response: ObjectApiResponse, datasets: Datasets
) -> Generator[Entity, None, None]:
    hits = response.get("hits", {})
    for hit in hits.get("hits", []):
        entity = result_entity(datasets, hit)
        if entity is not None:
            yield entity


def result_facets(response: ObjectApiResponse, datasets: Datasets):
    facets = {}
    for field, agg in response.get("aggregations", {}).items():
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
    return facets


async def search_entities(
    query: Dict[Any, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: Optional[Dict] = None,
    sort: List[Any] = [],
) -> Union[ObjectApiResponse, ApiError]:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    try:
        response = await es_.search(
            index=settings.ENTITY_INDEX,
            query=query,
            size=limit,
            sort=sort,
            from_=offset,
            aggregations=aggregations,
        )
        return response
    except ApiError as error:
        log.error(
            f"Search error {error.status_code}: {error.message}",
            index=settings.ENTITY_INDEX,
            query=query,
        )
        return error


async def statement_results(
    query: Dict[str, Any], limit: int, offset: int, sort: List[Any]
) -> Dict[str, Any]:
    es = await get_es()
    results = []
    es_ = es.options(opaque_id=get_opaque_id())
    resp = await es_.search(
        index=settings.STATEMENT_INDEX,
        query=query,
        size=limit,
        from_=offset,
        sort=sort,
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
        return result_entity(datasets, data)
    except NotFoundError:
        return None


async def get_adjacent(
    entity: Entity, exclude: List[str]
) -> AsyncGenerator[Tuple[Property, Entity], None]:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    entities = entity.get_type_values(registry.entity)
    entities = [e for e in entities if e not in exclude]
    datasets = await get_datasets()
    if len(entities):
        resp = await es_.mget(
            index=settings.ENTITY_INDEX,
            ids=entities,
            realtime=False,
        )
        for raw in resp.get("docs", []):
            adj = result_entity(datasets, raw)
            if adj is None:
                continue
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == adj.id:
                    yield prop, adj

    # Disable scoring by using a filter query
    query = {
        "bool": {
            "filter": [{"term": {registry.entity.group: entity.id}}],
            "must_not": [{"ids": {"values": exclude}}],
        }
    }
    resp = await es_.search(
        index=settings.ENTITY_INDEX,
        query=query,
        size=settings.MAX_RESULTS,
    )
    for adj in result_entities(resp, datasets):
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
    async for prop, adjacent in get_adjacent(entity, next_path):
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
