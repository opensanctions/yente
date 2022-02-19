import json
import time
import structlog
from uuid import uuid4
from normality import slugify
from structlog.contextvars import clear_contextvars, bind_contextvars
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from async_timeout import asyncio
from fastapi import FastAPI, Path, Query, Form
from fastapi import Request, Response
from fastapi import HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from followthemoney import model
from followthemoney.types import registry
from followthemoney.exc import InvalidData

from yente import settings
from yente.entity import Dataset
from yente.models import EntityExample, HealthzResponse
from yente.models import EntityMatchQuery, EntityMatchResponse
from yente.models import EntityResponse, SearchResponse
from yente.models import FreebaseEntitySuggestResponse
from yente.models import FreebasePropertySuggestResponse
from yente.models import FreebaseTypeSuggestResponse
from yente.models import FreebaseManifest, FreebaseQueryResult
from yente.models import StatementResponse
from yente.search.queries import text_query, entity_query, prefix_query
from yente.search.queries import facet_aggregations, statement_query
from yente.search.search import get_entity, query_entities, query_results
from yente.search.search import serialize_entity, get_index_status
from yente.search.search import statement_results
from yente.search.indexer import update_index
from yente.search.base import get_es
from yente.data import get_datasets
from yente.data import get_freebase_type, get_freebase_types
from yente.data import get_freebase_entity, get_freebase_property
from yente.data import get_matchable_schemata
from yente.util import match_prefix, limit_window, EntityRedirect


log: structlog.stdlib.BoundLogger = structlog.get_logger("yente")
app = FastAPI(
    title=settings.TITLE,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    contact=settings.CONTACT,
    openapi_tags=settings.TAGS,
    redoc_url="/",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

PATH_DATASET = Path(
    settings.SCOPE_DATASET,
    description="Data source or collection name",
    example=settings.SCOPE_DATASET,
)
QUERY_PREFIX = Query("", min_length=1, description="Search prefix")
MATCH_PAGE = 5


async def get_dataset(name: str) -> Dataset:
    datasets = await get_datasets()
    dataset = datasets.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start_time = time.time()
    user_id = request.headers.get("authorization")
    if user_id is not None:
        if " " in user_id:
            _, user_id = user_id.split(" ", 1)
        user_id = slugify(user_id)
    trace_id = uuid4().hex
    bind_contextvars(
        user_id=user_id,
        trace_id=trace_id,
        client_ip=request.client.host,
    )
    response = cast(Response, await call_next(request))
    time_delta = time.time() - start_time
    response.headers["x-trace-id"] = trace_id
    if user_id is not None:
        response.headers["x-user-id"] = user_id
    log.info(
        str(request.url.path),
        action="request",
        method=request.method,
        path=request.url.path,
        query=request.url.query,
        agent=request.headers.get("user-agent"),
        referer=request.headers.get("referer"),
        code=response.status_code,
        took=time_delta,
    )
    clear_contextvars()
    return response


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_index())


@app.on_event("shutdown")
async def shutdown_event():
    es = await get_es()
    await es.close()


@app.get(
    "/healthz",
    summary="Health check",
    tags=["System information"],
    response_model=HealthzResponse,
)
async def healthz():
    """No-op basic health check. This is used by cluster management systems like
    Kubernetes to verify the service is responsive."""
    ok = await get_index_status()
    if not ok:
        raise HTTPException(500, detail="Index not ready")
    return {"status": "ok"}


@app.post(
    "/updatez",
    summary="Force an index update",
    tags=["System information"],
    response_model=HealthzResponse,
)
async def force_update(
    background_tasks: BackgroundTasks,
    token: str = Query("", title="Update token for authentication"),
    sync: bool = Query(False, title="Wait until indexing is complete"),
):
    """Force the index to be re-generated. Works only if the update token is provided
    (serves as an API key, and can be set in the container environment)."""
    if not len(token.strip()) or not len(settings.UPDATE_TOKEN):
        raise HTTPException(403, detail="Invalid token.")
    if token != settings.UPDATE_TOKEN:
        raise HTTPException(403, detail="Invalid token.")
    if sync:
        await update_index(force=True)
    else:
        background_tasks.add_task(update_index, force=True)
    return {"status": "ok"}


@app.get(
    "/search/{dataset}",
    summary="Simple entity search",
    tags=["Matching"],
    response_model=SearchResponse,
)
async def search(
    q: str = Query("", title="Query text"),
    dataset: str = PATH_DATASET,
    schema: str = Query(settings.BASE_SCHEMA, title="Types of entities that can match"),
    countries: List[str] = Query([], title="Filter by country code"),
    topics: List[str] = Query([], title="Filter by entity topics"),
    datasets: List[str] = Query([], title="Filter by data sources"),
    limit: int = Query(10, title="Number of results to return", max=settings.MAX_PAGE),
    offset: int = Query(0, title="Start at result", max=settings.MAX_PAGE),
    fuzzy: bool = Query(False, title="Enable fuzzy matching"),
    nested: bool = Query(False, title="Include adjacent entities in response"),
):
    """Search endpoint for matching entities based on a simple piece of text, e.g.
    a name. This can be used to implement a simple, user-facing search. For proper
    entity matching, the multi-property matching API should be used instead."""
    limit, offset = limit_window(limit, offset, 10)
    ds = await get_dataset(dataset)
    schema_obj = model.get(schema)
    if schema_obj is None:
        raise HTTPException(400, detail="Invalid schema")
    filters = {"countries": countries, "topics": topics, "datasets": datasets}
    query = text_query(ds, schema_obj, q, filters=filters, fuzzy=fuzzy)
    aggregations = facet_aggregations([f for f in filters.keys()])
    resp = await query_results(
        query,
        limit=limit,
        offset=offset,
        nested=nested,
        aggregations=aggregations,
    )
    log.info(
        "Query",
        action="search",
        query=q,
        dataset=ds.name,
        total=resp.get("total"),
    )
    return JSONResponse(content=resp, headers=settings.CACHE_HEADERS)


async def _match_one(
    name: str,
    ds: Dataset,
    example: EntityExample,
    fuzzy: bool,
    limit: int,
) -> Tuple[str, Dict[str, Any]]:
    data = example.dict()
    data["id"] = "sample"
    data["schema"] = data.pop("schema_", data.pop("schema", None))
    entity = model.get_proxy(data, cleaned=False)
    query = entity_query(ds, entity, fuzzy=fuzzy)
    results = await query_results(query, limit=limit, offset=0, nested=False)
    results["query"] = entity.to_dict()
    log.info("Match", action="match", schema=data["schema"])
    return (name, results)


@app.post(
    "/match/{dataset}",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatchResponse,
)
async def match(
    match: EntityMatchQuery,
    dataset: str = PATH_DATASET,
    limit: int = Query(
        MATCH_PAGE,
        title="Number of results to return",
        lt=settings.MAX_PAGE,
    ),
    fuzzy: bool = Query(False, title="Enable n-gram matching of partial names"),
):
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting a batch of entities, each
    formatted like those returned by the API.

    Tutorial: [Using the matching API to do KYC-style checks](/articles/2022-02-01-matching-api/).

    For example, the following would be valid query examples:

    ```json
    "queries": {
        "entity1": {
            "schema": "Person",
            "properties": {
                "name": ["John Doe"],
                "birthDate": ["1975-04-21"],
                "nationality": ["us"]
            }
        },
        "entity2": {
            "schema": "Company",
            "properties": {
                "name": ["Brilliant Amazing Limited"],
                "jurisdiction": ["hk"],
                "registrationNumber": ["84BA99810"]
            }
        }
    }
    ```
    The values for `entity1`, `entity2` can be chosen freely to correlate results
    on the client side when the request is returned. The responses will be given
    for each submitted example like this:

    ```json
    "responses": {
        "entity1": {
            "query": {},
            "results": [...]
        },
        "entity2": {
            "query": {},
            "results": [...]
        }
    }
    ```

    The precision of the results will be dependent on the amount of detail submitted
    with each example. The following properties are most helpful for particular types:

    * **Person**: ``name``, ``birthDate``, ``nationality``, ``idNumber``, ``address``
    * **Organization**: ``name``, ``country``, ``registrationNumber``, ``address``
    * **Company**: ``name``, ``jurisdiction``, ``registrationNumber``, ``address``,
      ``incorporationDate``
    """
    ds = await get_dataset(dataset)
    limit, _ = limit_window(limit, 0, 10)
    tasks = []
    for name, example in match.queries.items():
        tasks.append(_match_one(name, ds, example, fuzzy, limit))
    if not len(tasks):
        raise HTTPException(400, "No queries provided.")
    responses = await asyncio.gather(*tasks)
    return {"responses": {n: r for n, r in responses}}


@app.get("/entities/{entity_id}", tags=["Data access"], response_model=EntityResponse)
async def fetch_entity(
    entity_id: str = Path(None, description="ID of the entity to retrieve"),
):
    """Retrieve a single entity by its ID. The entity will be returned in
    full, with data from all datasets and with nested entities (adjacent
    passport, sanction and associated entities) included.

    Intro: [entity data model](https://www.opensanctions.org/docs/entities/).
    """
    try:
        entity = await get_entity(entity_id)
    except EntityRedirect as redir:
        url = app.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    data = await serialize_entity(entity, nested=True)
    log.info(data.get("caption"), action="entity", entity_id=entity_id)
    return JSONResponse(content=data, headers=settings.CACHE_HEADERS)


@app.get(
    "/statements",
    summary="Statement-based records",
    tags=["Data access"],
    response_model=StatementResponse,
)
async def statements(
    dataset: Optional[str] = Query(None, title="Filter by dataset"),
    entity_id: Optional[str] = Query(None, title="Filter by source entity ID"),
    canonical_id: Optional[str] = Query(None, title="Filter by normalised entity ID"),
    prop: Optional[str] = Query(None, title="Filter by property name"),
    value: Optional[str] = Query(None, title="Filter by property value"),
    schema: Optional[str] = Query(None, title="Filter by schema type"),
    limit: int = Query(
        50,
        title="Number of results to return",
        lt=settings.MAX_PAGE,
    ),
    offset: int = Query(
        0,
        title="Number of results to skip before returning them",
        lt=settings.MAX_PAGE,
    ),
):
    """Access raw entity data as statements.

    Read [statement-based data model](https://www.opensanctions.org/docs/statements/)
    for context regarding this endpoint.
    """
    if not settings.STATEMENT_API:
        raise HTTPException(501, "Statement API not enabled.")
    ds = None
    if dataset is not None:
        ds = await get_dataset(dataset)
    query = statement_query(
        dataset=ds,
        entity_id=entity_id,
        canonical_id=canonical_id,
        prop=prop,
        value=value,
        schema=schema,
    )
    limit, offset = limit_window(limit, offset, 50)
    resp = await statement_results(query, limit, offset)
    return JSONResponse(content=resp, headers=settings.CACHE_HEADERS)


@app.get(
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


@app.post(
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

    tasks = []
    for k, q in queries.items():
        tasks.append(reconcile_query(k, dataset, q))
    results = await asyncio.gather(*tasks)
    return {k: r for (k, r) in results}


async def reconcile_query(name: str, dataset: Dataset, query: Dict[str, Any]):
    """Reconcile operation for a single query."""
    # log.info("Reconcile: %r", query)
    limit, offset = limit_window(query.get("limit"), 0, MATCH_PAGE)
    type = query.get("type", settings.BASE_SCHEMA)
    proxy = model.make_entity(type)
    proxy.add("alias", query.get("query"))
    for p in query.get("properties", []):
        prop = model.get_qname(p.get("pid"))
        if prop is None:
            continue
        try:
            proxy.add(prop.name, p.get("v"), fuzzy=True)
        except InvalidData:
            log.exception("Invalid property is set.")

    results = []
    # log.info("QUERY %r %s", proxy.to_dict(), limit)
    query = entity_query(dataset, proxy, fuzzy=True)
    async for result, score in query_entities(query, limit=limit, offset=offset):
        results.append(get_freebase_entity(result, score))
    log.info("Reconcile", action="match", schema=proxy.schema.name)
    return name, {"result": results}


@app.get(
    "/reconcile/{dataset}/suggest/entity",
    summary="Suggest entity",
    tags=["Reconciliation"],
    response_model=FreebaseEntitySuggestResponse,
)
async def reconcile_suggest_entity(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    limit: int = Query(
        MATCH_PAGE,
        description="Number of suggestions to return",
        lt=settings.MAX_PAGE,
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
    limit, offset = limit_window(limit, 0, MATCH_PAGE)
    async for result, score in query_entities(query, limit=limit, offset=offset):
        results.append(get_freebase_entity(result, score))
    return {
        "prefix": prefix,
        "result": results,
    }


@app.get(
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
    matches = []
    for prop in model.properties:
        if prop.schema not in schemata:
            continue
        if prop.hidden or prop.type == prop.type == registry.entity:
            continue
        if match_prefix(prefix, prop.name, prop.label):
            matches.append(get_freebase_property(prop))
    return {
        "prefix": prefix,
        "result": matches,
    }


@app.get(
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
    matches = []
    for schema in await get_matchable_schemata():
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(get_freebase_type(schema))
    return {
        "prefix": prefix,
        "result": matches,
    }
