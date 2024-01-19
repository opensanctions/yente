import json
from typing import Generator, Set
from typing import Any, Dict, List, Optional
from elasticsearch import TransportError, ApiError
from elasticsearch.exceptions import NotFoundError
from elastic_transport import ObjectApiResponse
from fastapi import HTTPException
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry
from nomenklatura.dataset import DataCatalog

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.entity import Entity
from yente.data.common import SearchFacet, SearchFacetItem, TotalSpec
from yente.search.base import get_es, get_opaque_id, query_semaphore
from yente.util import EntityRedirect

log = get_logger(__name__)


def result_entity(data: Dict[str, Any]) -> Optional[Entity]:
    source: Optional[Dict[str, Any]] = data.get("_source")
    if source is None or source.get("schema") is None:
        return None
    source["id"] = data.get("_id")
    entity = Entity.from_dict(model, source)
    entity.datasets = set(source["datasets"])
    return entity


def result_total(result: ObjectApiResponse[Any]) -> TotalSpec:
    total: Dict[str, Any] = result.get("hits", {}).get("total")
    return TotalSpec(value=total["value"], relation=total["relation"])


def result_entities(response: ObjectApiResponse[Any]) -> Generator[Entity, None, None]:
    hits = response.get("hits", {})
    for hit in hits.get("hits", []):
        entity = result_entity(hit)
        if entity is not None:
            yield entity


def result_facets(
    response: ObjectApiResponse[Any], catalog: DataCatalog[Dataset]
) -> Dict[str, SearchFacet]:
    facets: Dict[str, SearchFacet] = {}
    aggs: Dict[str, Dict[str, Any]] = response.get("aggregations", {})
    for field, agg in aggs.items():
        facet = SearchFacet(label=field, values=[])
        buckets: List[Dict[str, Any]] = agg.get("buckets", [])
        for bucket in buckets:
            key: Optional[str] = bucket.get("key")
            if key is not None:
                key = str(key)
            count: Optional[int] = bucket.get("doc_count")
            if key is None or count is None:
                continue
            value = SearchFacetItem(name=key, label=key, count=count)
            if field == "datasets":
                facet.label = "Data sources"
                value.label = key
                ds = catalog.get(key)
                if ds is not None:
                    value.label = ds.title or key
            if field == "schema":
                facet.label = "Entity types"
                value.label = key
                schema_obj = model.get(key)
                if schema_obj is not None:
                    value.label = schema_obj.plural
            if field in registry.groups:
                type_ = registry.groups[field]
                facet.label = type_.plural
                value.label = type_.caption(key) or value.label
            facet.values.append(value)
        facets[field] = facet
    return facets


async def search_entities(
    query: Dict[str, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: Optional[Dict[str, Any]] = None,
    sort: List[Any] = [],
) -> ObjectApiResponse[Any]:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    try:
        async with query_semaphore:
            response = await es_.search(
                index=settings.ENTITY_INDEX,
                query=query,
                size=limit,
                sort=sort,
                from_=offset,
                aggregations=aggregations,
                # https://discuss.elastic.co/t/querying-an-alias-throws-off-scoring-completely/351423/4
                search_type="dfs_query_then_fetch",
            )
            return response
    except ApiError as ae:
        log.warning(
            f"API error {ae.status_code}: {ae.message}",
            index=settings.ENTITY_INDEX,
            query_json=json.dumps(query),
        )
        raise HTTPException(status_code=ae.status_code, detail=ae.body)


async def get_entity(entity_id: str) -> Optional[Entity]:
    es = await get_es()
    try:
        es_ = es.options(opaque_id=get_opaque_id())
        query = {
            "bool": {
                "should": [
                    {"ids": {"values": [entity_id]}},
                    {"term": {"referents": {"value": entity_id}}},
                ],
                "minimum_should_match": 1,
            }
        }
        async with query_semaphore:
            response = await es_.search(
                index=settings.ENTITY_INDEX,
                query=query,
                size=2,
            )
        hits = response.get("hits", {})
        for hit in hits.get("hits", []):
            if hit.get("_id") != entity_id:
                raise EntityRedirect(hit.get("_id"))
            entity = result_entity(hit)
            if entity is not None:
                return entity
    except NotFoundError:
        pass
    except ApiError as ae:
        msg = f"API error {ae.status_code}: {str(ae)}"
        log.warning(msg, index=settings.ENTITY_INDEX)
        raise HTTPException(status_code=ae.status_code, detail=ae.message)
    return None


async def get_matchable_schemata(dataset: Dataset) -> Set[Schema]:
    """Get the set of schema used in this dataset that are matchable or
    a parent schema to a matchable schema."""
    filter_ = {"terms": {"datasets": dataset.dataset_names}}
    facet = "schemata"
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    try:
        async with query_semaphore:
            response = await es_.search(
                index=settings.ENTITY_INDEX,
                query={"bool": {"filter": [filter_]}},
                size=0,
                aggregations={facet: {"terms": {"field": "schema", "size": 1000}}},
            )
        aggs = response.get("aggregations", {})
        schemata: Set[Schema] = set()
        for bucket in aggs.get(facet, {}).get("buckets", []):
            key = bucket.get("key")
            schema = model.get(key)
            if schema is not None and schema.matchable:
                schemata.update(schema.schemata)
        return schemata
    except ApiError as error:
        log.error("Could not get matchable schema", error=str(error))
        return set()


async def get_index_status(index: Optional[str] = None) -> bool:
    es = await get_es()
    try:
        es_ = es.options(request_timeout=5, opaque_id=get_opaque_id())
        health = await es_.cluster.health(index=index, timeout=0)
        status = health.get("status")
        if status not in ("yellow", "green"):
            log.warning("Index is not in green state")
            return False
        return True
    except (ApiError, TransportError) as te:
        log.error(f"Search status failure: {te}")
        return False
