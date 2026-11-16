import time
from collections.abc import Generator, Mapping
from typing import Any

from followthemoney import Schema, model, registry
from followthemoney.dataset import DataCatalog
from opentelemetry import metrics
from opentelemetry.util.types import AttributeValue

from yente import settings
from yente.data.common import SearchFacet, SearchFacetItem, TotalSpec
from yente.data.dataset import Dataset
from yente.data.entity import Entity
from yente.logs import get_logger
from yente.provider import SearchProvider
from yente.util import EntityRedirect, limit_window

log = get_logger(__name__)
AggType = dict[str, dict[str, list[dict[str, Any]]]]

# Fine steps between 10ms and 1s, where match and search queries land, so
# percentiles interpolated from the buckets stay within about 25%.
# fmt: off
_QUERY_DURATION_BUCKETS = [
    0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.06, 0.075,
    0.1, 0.125, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75,
    1, 1.5, 2.5, 5, 10,
]
# fmt: on
_meter = metrics.get_meter("yente.search")
_query_duration = _meter.create_histogram(
    "yente.search.query_duration",
    unit="s",
    description="Duration of entity search queries against the search backend",
    explicit_bucket_boundaries_advisory=_QUERY_DURATION_BUCKETS,
)


def result_entity(data: dict[str, Any]) -> Entity | None:
    source: dict[str, Any] | None = data.get("_source")
    if source is None or source.get("schema") is None:
        return None
    source["id"] = data.get("_id")
    return Entity.from_dict(source)


def result_total(result: dict[str, Any]) -> TotalSpec:
    total: dict[str, Any] = result.get("hits", {}).get("total")
    return TotalSpec(value=total["value"], relation=total["relation"])


def result_entities(
    response: dict[str, Any],
) -> Generator[tuple[Entity, float], None, None]:
    hits = response.get("hits", {})
    for hit in hits.get("hits", []):
        entity = result_entity(hit)
        score = float(hit.get("_score") or 0.0)
        if entity is not None:
            yield (entity, score)


def result_facets(
    response: dict[str, Any], catalog: DataCatalog[Dataset]
) -> dict[str, SearchFacet]:
    facets: dict[str, SearchFacet] = {}
    aggs: AggType = response.get("aggregations", {})
    for field, agg in aggs.items():
        facet = SearchFacet(label=field, values=[])
        buckets: list[dict[str, Any]] = agg.get("buckets", [])
        for bucket in buckets:
            key: str | None = bucket.get("key")
            if key is not None:
                key = str(key)
            count: int | None = bucket.get("doc_count")
            if key is None or count is None:
                continue
            value = SearchFacetItem(name=key, label=key, count=count)
            if field == "datasets":
                facet.label = "Data sources"
                value.label = key
                ds = catalog.get(key)
                if ds is not None:
                    value.label = ds.model.title or key
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


def upscore_large_entities(query: dict[str, Any]) -> dict[str, Any]:
    """Wrap query to up-score important entities."""

    return {
        "function_score": {
            "query": query,
            "functions": [
                {
                    "field_value_factor": {
                        "field": "entity_values_count",
                        # This is a bit of a jiggle factor. Currently, very large documents (like Vladimir Putin)
                        # have a entity_values_count of ~200, so get a +10 boost.
                        # The order is modifier(factor * value)
                        "factor": 0.5,
                        "modifier": "sqrt",
                        # Used only if this is an old index that doesn't have entity_values_count yet
                        # (until the first reindex after the upgrade is completed).
                        "missing": 0,
                    }
                }
            ],
            "boost_mode": "sum",
        }
    }


async def search_entities(
    provider: SearchProvider,
    query: dict[str, Any],
    limit: int = 5,
    offset: int = 0,
    aggregations: dict[str, Any] | None = None,
    sort: list[Any] | None = None,
    track_total_hits: bool = True,
    metric_attributes: Mapping[str, AttributeValue] | None = None,
) -> dict[str, Any]:
    limit, offset = limit_window(limit, offset)

    start = time.perf_counter()
    response = await provider.search(
        index=settings.ENTITY_INDEX,
        query=query,
        size=limit,
        sort=sort,
        from_=offset,
        aggregations=aggregations,
        rank_precise=True,
        track_total_hits=track_total_hits,
    )
    _query_duration.record(time.perf_counter() - start, metric_attributes)
    return response


async def get_entity(provider: SearchProvider, entity_id: str) -> Entity | None:
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
) -> set[Schema]:
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
    schemata: set[Schema] = set()
    for bucket in aggs.get(facet, {}).get("buckets", []):
        schema = model.get(bucket["key"])
        if schema is not None and schema.matchable:
            schemata.update(schema.schemata)
    return schemata
