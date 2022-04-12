import json
import asyncio
import structlog
from structlog.stdlib import BoundLogger
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Query, Form
from fastapi import Request
from fastapi import HTTPException
from followthemoney import model
from followthemoney.types import registry
from elasticsearch import ApiError

from yente import settings
from yente.entity import Dataset
from yente.models import (
    FreebaseEntity,
    FreebaseEntitySuggestResponse,
    FreebaseProperty,
    FreebaseType,
)
from yente.models import FreebasePropertySuggestResponse
from yente.models import FreebaseTypeSuggestResponse
from yente.models import FreebaseManifest, FreebaseQueryResult
from yente.search.queries import entity_query, prefix_query
from yente.search.search import search_entities, result_entities, result_total
from yente.data import get_datasets, get_freebase_type, get_freebase_types
from yente.data import get_freebase_entity, get_freebase_scored
from yente.data import get_matchable_schemata, get_freebase_property
from yente.scoring import prepare_entity, score_results
from yente.util import match_prefix, limit_window
from yente.routers.util import PATH_DATASET, QUERY_PREFIX, get_dataset


log: BoundLogger = structlog.get_logger(__name__)
router = APIRouter()


@router.get(
    "/reconcile/{dataset}",
    summary="Reconciliation info",
    tags=["Reconciliation"],
    response_model=Union[FreebaseManifest, FreebaseQueryResult],
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
        "defaultTypes": await get_freebase_types(),
    }


@router.post(
    "/reconcile/{dataset}",
    summary="Reconciliation queries",
    tags=["Reconciliation"],
    response_model=FreebaseQueryResult,
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
    except ValueError:
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
    datasets = await get_datasets()
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

    data = {"schema": schema, "properties": properties}
    proxy = prepare_entity(data)
    query = entity_query(dataset, proxy)
    resp = await search_entities(query, limit=limit, offset=offset)
    if isinstance(resp, ApiError):
        raise HTTPException(resp.status_code, detail=resp.message)
    entities = result_entities(resp, datasets)
    results = [get_freebase_scored(r) for r in score_results(proxy, entities)]
    log.info(
        "Reconcile",
        action="reconcile",
        schema=proxy.schema.name,
        total=result_total(resp),
    )
    return name, {"result": results}


@router.get(
    "/reconcile/{dataset}/suggest/entity",
    summary="Suggest entity",
    tags=["Reconciliation"],
    response_model=FreebaseEntitySuggestResponse,
)
async def reconcile_suggest_entity(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    limit: int = Query(
        settings.MATCH_PAGE,
        description="Number of suggestions to return",
        lte=settings.MAX_PAGE,
    ),
):
    """Suggest an entity based on a text query. This is functionally very
    similar to the basic search API, but returns data in the structure assumed
    by the community specification.

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    ds = await get_dataset(dataset)
    datasets = await get_datasets()
    results = []
    query = prefix_query(ds, prefix)
    limit, offset = limit_window(limit, 0, settings.MATCH_PAGE)
    resp = await search_entities(query, limit=limit, offset=offset)
    if isinstance(resp, ApiError):
        raise HTTPException(resp.status_code, detail=resp.message)
    for result in result_entities(resp, datasets):
        results.append(get_freebase_entity(result))
    log.info(
        "Prefix query",
        action="suggest",
        length=len(prefix),
        dataset=ds.name,
        total=result_total(resp),
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
    await get_dataset(dataset)
    schemata = await get_matchable_schemata()
    matches: List[FreebaseProperty] = []
    for prop in model.properties:
        if prop.schema not in schemata:
            continue
        if prop.hidden or prop.type == prop.type == registry.entity:
            continue
        if match_prefix(prefix, prop.name, prop.label):
            matches.append(get_freebase_property(prop))
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
    await get_dataset(dataset)
    matches: List[FreebaseType] = []
    for schema in await get_matchable_schemata():
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(get_freebase_type(schema))
    result = matches[: settings.MATCH_PAGE]
    return FreebaseTypeSuggestResponse(prefix=prefix, result=result)
