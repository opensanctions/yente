from typing import Generator, Set
from typing import Any, Dict, List, Optional
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry
from nomenklatura.dataset import DataCatalog

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.entity import Entity
from yente.data.common import SearchFacet, SearchFacetItem, TotalSpec
from yente.provider import SearchProvider
from yente.util import EntityRedirect

log = get_logger(__name__)
AggType = Dict[str, Dict[str, List[Dict[str, Any]]]]


def result_entity(data: Dict[str, Any]) -> Optional[Entity]:
    source: Optional[Dict[str, Any]] = data.get("_source")
    if source is None or source.get("schema") is None:
        return None
    source["id"] = data.get("_id")
    entity = Entity.from_dict(model, source)
    entity.datasets = set(source["datasets"])
    return entity


def result_total(result: Dict[str, Any]) -> TotalSpec:
    total: Dict[str, Any] = result.get("hits", {}).get("total")
    return TotalSpec(value=total["value"], relation=total["relation"])


def result_entities(response: Dict[str, Any]) -> Generator[Entity, None, None]:
    hits = response.get("hits", {})
    for hit in hits.get("hits", []):
        entity = result_entity(hit)
        if entity is not None:
            yield entity


def result_facets(
    response: Dict[str, Any], catalog: DataCatalog[Dataset]
) -> Dict[str, SearchFacet]:
    facets: Dict[str, SearchFacet] = {}
    aggs: AggType = response.get("aggregations", {})
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
    provider: SearchProvider,
    query: Dict[str, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: Optional[Dict[str, Any]] = None,
    sort: List[Any] = [],
) -> Dict[str, Any]:
    return await provider.search(
        index=settings.ENTITY_INDEX,
        query=query,
        size=limit,
        sort=sort,
        from_=offset,
        aggregations=aggregations,
        rank_precise=True,
    )


async def get_entity(provider: SearchProvider, entity_id: str) -> Optional[Entity]:
    query = {
        "bool": {
            "should": [
                {"ids": {"values": [entity_id]}},
                {"term": {"referents": {"value": entity_id}}},
            ],
            "minimum_should_match": 1,
        }
    }
    response = await provider.search(
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
    return None


async def get_matchable_schemata(
    provider: SearchProvider, dataset: Dataset
) -> Set[Schema]:
    """Get the set of schema used in this dataset that are matchable or
    a parent schema to a matchable schema."""
    filter_ = {"terms": {"datasets": dataset.dataset_names}}
    facet = "schemata"
    response = await provider.search(
        index=settings.ENTITY_INDEX,
        query={"bool": {"filter": [filter_]}},
        size=0,
        aggregations={facet: {"terms": {"field": "schema", "size": 1000}}},
    )
    aggs: AggType = response.get("aggregations", {})
    schemata: Set[Schema] = set()
    for bucket in aggs.get(facet, {}).get("buckets", []):
        schema = model.get(bucket["key"])
        if schema is not None and schema.matchable:
            schemata.update(schema.schemata)
    return schemata
