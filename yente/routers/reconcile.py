import json
import asyncio
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Query, Form
from fastapi import Request
from fastapi import HTTPException
from followthemoney import model
from followthemoney.types import registry
from elasticsearch import ApiError

from yente import settings
from yente.data.common import ErrorResponse
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data.freebase import (
    FreebaseEntity,
    FreebaseProperty,
    FreebaseScoredEntity,
    FreebaseType,
    FreebaseEntitySuggestResponse,
    FreebasePropertySuggestResponse,
    FreebaseTypeSuggestResponse,
    FreebaseManifest,
    FreebaseQueryResult,
)
from yente.search.queries import entity_query, prefix_query
from yente.search.search import search_entities, result_entities, result_total
from yente.search.search import get_matchable_schemata
from yente.scoring import score_results
from yente.util import match_prefix, limit_window
from yente.routers.util import PATH_DATASET, QUERY_PREFIX, get_dataset


log = get_logger(__name__)
router = APIRouter()


@router.get(
    "/reconcile/{dataset}",
    summary="Reconciliation info",
    tags=["Reconciliation"],
    response_model=Union[FreebaseManifest, FreebaseQueryResult],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def reconcile(
    request: Request,
    queries: Optional[str] = None,
    dataset: str = PATH_DATASET,
):
    """Reconciliation API, emulates Google Refine API. This endpoint can be used
    to bulk match entities against the system using an end-user application like
    [OpenRefine](https://openrefine.org).

    Tutorial: [Using OpenRefine to match entities in a spreadsheet](/articles/2022-01-10-openrefine-reconciliation/).
    """
    ds = await get_dataset(dataset)
    if queries is not None:
        return await reconcile_queries(ds, queries)
    base_url = urljoin(str(request.base_url), f"/reconcile/{dataset}")
    schemata = await get_matchable_schemata(ds)
    default_types = [FreebaseType.from_schema(s) for s in schemata]
    return {
        "versions": ["0.2"],
        "name": f"{ds.title} ({settings.TITLE})",
        "identifierSpace": "https://opensanctions.org/reference/#schema",
        "schemaSpace": "https://opensanctions.org/reference/#schema",
        "view": {"url": ("https://opensanctions.org/entities/{{id}}/")},
        "preview": {
            "url": "https://opensanctions.org/entities/preview/{{id}}/",
            "width": 430,
            "height": 300,
        },
        "suggest": {
            "entity": {
                "service_url": base_url,
                "service_path": "/suggest/entity",
            },
            "type": {
                "service_url": base_url,
                "service_path": "/suggest/type",
            },
            "property": {
                "service_url": base_url,
                "service_path": "/suggest/property",
            },
        },
        "defaultTypes": default_types,
    }


@router.post(
    "/reconcile/{dataset}",
    summary="Reconciliation queries",
    tags=["Reconciliation"],
    response_model=FreebaseQueryResult,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def reconcile_post(
    dataset: str = PATH_DATASET,
    queries: str = Form(None, description="JSON-encoded reconciliation queries"),
):
    """Reconciliation API, emulates Google Refine API. This endpoint is used by
    clients for matching, refer to the discovery endpoint for details."""
    ds = await get_dataset(dataset)
    return await reconcile_queries(ds, queries)


async def reconcile_queries(
    dataset: Dataset,
    data: str,
):
    # multiple requests in one query
    try:
        queries = json.loads(data)
    except (TypeError, ValueError):
        raise HTTPException(400, detail="Cannot decode query")

    if len(queries) > settings.MAX_BATCH:
        msg = "Too many queries in one batch (limit: %d)" % settings.MAX_BATCH
        raise HTTPException(400, detail=msg)

    tasks = []
    for k, q in queries.items():
        tasks.append(reconcile_query(k, dataset, q))
    results = await asyncio.gather(*tasks)
    return {k: r for (k, r) in results}


async def reconcile_query(name: str, dataset: Dataset, query: Dict[str, Any]):
    """Reconcile operation for a single query."""
    # log.info("Reconcile: %r", query)
    limit, offset = limit_window(query.get("limit"), 0, settings.MAX_MATCHES)
    schema = query.get("type", settings.BASE_SCHEMA)
    properties = {"alias": [query.get("query")]}

    for p in query.get("properties", []):
        prop = model.get_qname(p.get("pid"))
        if prop is None:
            continue
        if prop.name not in properties:
            properties[prop.name] = []
        properties[prop.name].append(p.get("v"))

    proxy = Entity.from_example(schema, properties)
    query = entity_query(dataset, proxy)
    resp = await search_entities(query, limit=limit, offset=offset)
    entities = result_entities(resp)
    results = [
        FreebaseScoredEntity.from_scored(r) for r in score_results(proxy, entities)
    ]
    log.info(
        f"/reconcile/{dataset.name}",
        action="reconcile",
        schema=proxy.schema.name,
        results=result_total(resp).value,
    )
    return name, {"result": results}


@router.get(
    "/reconcile/{dataset}/suggest/entity",
    summary="Suggest entity",
    tags=["Reconciliation"],
    response_model=FreebaseEntitySuggestResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def reconcile_suggest_entity(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    limit: int = Query(
        settings.MATCH_PAGE,
        description="Number of suggestions to return",
        le=settings.MAX_PAGE,
    ),
):
    """Suggest an entity based on a text query. This is functionally very
    similar to the basic search API, but returns data in the structure assumed
    by the community specification.

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    ds = await get_dataset(dataset)
    results = []
    query = prefix_query(ds, prefix)
    limit, offset = limit_window(limit, 0, settings.MATCH_PAGE)
    resp = await search_entities(query, limit=limit, offset=offset)
    for result in result_entities(resp):
        results.append(FreebaseEntity.from_proxy(result))
    log.info(
        f"/reconcile/{ds.name}/suggest/entity",
        action="suggest",
        length=len(prefix),
        dataset=ds.name,
        results=result_total(resp).value,
    )
    return FreebaseEntitySuggestResponse(prefix=prefix, result=results)


@router.get(
    "/reconcile/{dataset}/suggest/property",
    summary="Suggest property",
    tags=["Reconciliation"],
    response_model=FreebasePropertySuggestResponse,
)
async def reconcile_suggest_property(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
):
    """Given a search prefix, return all the type/schema properties which match
    the given text. This is used to auto-complete property selection for detail
    filters in OpenRefine."""
    ds = await get_dataset(dataset)
    schemata = await get_matchable_schemata(ds)
    matches: List[FreebaseProperty] = []
    for prop in model.properties:
        if prop.schema not in schemata:
            continue
        if prop.hidden or prop.type == prop.type == registry.entity:
            continue
        if match_prefix(prefix, prop.name, prop.label):
            matches.append(FreebaseProperty.from_prop(prop))
    result = matches[: settings.MATCH_PAGE]
    return FreebasePropertySuggestResponse(prefix=prefix, result=result)


@router.get(
    "/reconcile/{dataset}/suggest/type",
    summary="Suggest type (schema)",
    tags=["Reconciliation"],
    response_model=FreebaseTypeSuggestResponse,
)
async def reconcile_suggest_type(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
):
    """Given a search prefix, return all the types (i.e. schema) which match
    the given text. This is used to auto-complete type selection for the
    configuration of reconciliation in OpenRefine."""
    ds = await get_dataset(dataset)
    matches: List[FreebaseType] = []
    for schema in await get_matchable_schemata(ds):
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(FreebaseType.from_schema(schema))
    result = matches[: settings.MATCH_PAGE]
    return FreebaseTypeSuggestResponse(prefix=prefix, result=result)
