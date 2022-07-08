import json
from typing import Generator, Set, Union
from typing import Any, Dict, List, Optional
from elasticsearch import TransportError, ApiError
from elasticsearch.exceptions import NotFoundError
from elastic_transport import ObjectApiResponse
from fastapi import HTTPException
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset, Datasets
from yente.data.entity import Entity
from yente.data.common import SearchFacet, SearchFacetItem, TotalSpec
from yente.search.base import get_es, get_opaque_id, semaphore
from yente.util import EntityRedirect

log = get_logger(__name__)


def result_entity(data) -> Optional[Entity]:
    source = data.get("_source")
    if source is None or source.get("schema") is None:
        return None
    source["id"] = data.get("_id")
    return Entity.from_dict(model, source)


def result_total(result: ObjectApiResponse) -> TotalSpec:
    spec = result.get("hits", {}).get("total")
    return TotalSpec(value=spec["value"], relation=spec["relation"])


def result_entities(response: ObjectApiResponse) -> Generator[Entity, None, None]:
    hits = response.get("hits", {})
    for hit in hits.get("hits", []):
        entity = result_entity(hit)
        if entity is not None:
            yield entity


def result_facets(response: ObjectApiResponse, datasets: Datasets):
    facets: Dict[str, SearchFacet] = {}
    for field, agg in response.get("aggregations", {}).items():
        facet = SearchFacet(label=field, values=[])
        for bucket in agg.get("buckets", []):
            key = bucket.get("key")
            value = SearchFacetItem(name=key, label=key, count=bucket.get("doc_count"))
            if field == "datasets":
                facet.label = "Data sources"
                try:
                    value.label = datasets[key].title
                except KeyError:
                    value.label = key
            if field in registry.groups:
                type_ = registry.groups[field]
                facet.label = type_.plural
                value.label = type_.caption(key) or value.label
            facet.values.append(value)
        facets[field] = facet
    return facets


async def search_entities(
    query: Dict[Any, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: Optional[Dict] = None,
    sort: List[Any] = [],
) -> ObjectApiResponse:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    try:
        async with semaphore:
            response = await es_.search(
                index=settings.ENTITY_INDEX,
                query=query,
                size=limit,
                sort=sort,
                from_=offset,
                aggregations=aggregations,
            )
            return response
    except ApiError as ae:
        log.warning(
            f"API error {ae.status_code}: {str(ae)}",
            index=settings.ENTITY_INDEX,
            query_json=json.dumps(query),
        )
        raise HTTPException(status_code=ae.status_code, detail=ae.message)


async def get_entity(entity_id: str) -> Optional[Entity]:
    es = await get_es()
    try:
        es_ = es.options(opaque_id=get_opaque_id())
        query = {"bool": {"filter": [{"ids": {"values": [entity_id]}}]}}
        async with semaphore:
            response = await es_.search(
                index=settings.ENTITY_INDEX,
                query=query,
                size=2,
            )
        hits = response.get("hits", {})
        for hit in hits.get("hits", []):
            _source = hit.get("_source")
            if _source.get("canonical_id") != entity_id:
                raise EntityRedirect(_source.get("canonical_id"))
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
        async with semaphore:
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


async def get_index_status() -> bool:
    es = await get_es()
    try:
        es_ = es.options(request_timeout=5, opaque_id=get_opaque_id())
        health = await es_.cluster.health()
        status = health.get("status")
        if status not in ("yellow", "green"):
            log.warning("ElasticSearch is not in green state")
            return False
        return True
    except TransportError as te:
        log.error(f"Healthz status failure: {te}")
        return False
