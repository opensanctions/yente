import json
import logging
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Tuple, Union
from async_timeout import asyncio
from fastapi import FastAPI, Path, Query, Form
from fastapi import HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from followthemoney.types import registry
from starlette.responses import RedirectResponse
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.exc import InvalidData

from yente import settings
from yente.entity import Dataset
from yente.models import HealthzResponse, IndexResponse
from yente.models import EntityMatchQuery, EntityMatchResponse
from yente.models import EntityResponse, SearchResponse
from yente.models import FreebaseEntitySuggestResponse
from yente.models import FreebasePropertySuggestResponse
from yente.models import FreebaseTypeSuggestResponse
from yente.models import FreebaseManifest, FreebaseQueryResult
from yente.models import StatementResponse
from yente.models import MAX_LIMIT
from yente.queries import statement_query, text_query, entity_query, facet_aggregations
from yente.search import get_entity, query_entities, query_results, statement_results
from yente.search import serialize_entity, get_index_status, get_index_stats
from yente.indexer import update_index
from yente.data import get_datasets
from yente.data import get_freebase_type, get_freebase_types
from yente.data import get_freebase_entity, get_freebase_property
from yente.data import get_matchable_schemata, get_scope
from yente.util import match_prefix, EntityRedirect


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
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
QUERY_PREFIX = Query(None, min_length=1, description="Search prefix")


async def get_dataset(name: str) -> Dataset:
    datasets = await get_datasets()
    dataset = datasets.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset


@app.on_event("startup")
async def startup_event():
    # await update_index()
    pass


@app.get(
    "/info",
    summary="System information",
    tags=["System information"],
    response_model=IndexResponse,
)
async def index():
    """Get system information: the list of available dataset names, the size of
    the search index in memory, and the followthemoney model specification which
    describes the types of entities and properties in use by the API."""
    datasets = await get_datasets()
    return {
        "datasets": [d for d in datasets.keys()],
        "model": model.to_dict(),
        "index": await get_index_stats(),
    }


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
):
    """Force the index to be re-generated. Works only if the update token is provided
    (serves as an API key, and can be set in the container environment)."""
    if not len(token.strip()) or not len(settings.UPDATE_TOKEN):
        raise HTTPException(403, detail="Invalid token.")
    if token != settings.UPDATE_TOKEN:
        raise HTTPException(403, detail="Invalid token.")
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
    limit: int = Query(10, title="Number of results to return", max=MAX_LIMIT),
    offset: int = Query(0, title="Start at result", max=MAX_LIMIT),
    fuzzy: bool = Query(False, title="Enable n-gram matching of partial names"),
    nested: bool = Query(False, title="Include adjacent entities in response"),
):
    """Search endpoint for matching entities based on a simple piece of text, e.g.
    a name. This can be used to implement a simple, user-facing search. For proper
    entity matching, the multi-property matching API should be used instead."""
    ds = await get_dataset(dataset)
    schema_obj = model.get(schema)
    if schema_obj is None:
        raise HTTPException(400, detail="Invalid schema")
    filters = {"countries": countries, "topics": topics, "datasets": datasets}
    query = text_query(ds, schema_obj, q, filters=filters, fuzzy=fuzzy)
    aggregations = facet_aggregations(filters.keys())
    return await query_results(
        ds, query, limit, aggregations=aggregations, nested=nested, offset=offset
    )


async def _match_one(
    name: str,
    ds: Dataset,
    example: Dict[str, Any],
    fuzzy: bool,
    limit: int,
    nested: bool,
) -> Tuple[str, Dict[str, Any]]:
    entity = EntityProxy.from_dict(model, example, cleaned=False)
    for prop, value in example.get("properties").items():
        entity.add(prop, value, cleaned=False)
    query = entity_query(ds, entity, fuzzy=fuzzy)
    results = await query_results(ds, query, limit, nested=nested)
    results["query"] = entity.to_dict()
    return (name, results)


@app.post(
    "/match/{dataset}",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatchResponse,
)
async def match(
    query: EntityMatchQuery,
    dataset: str = PATH_DATASET,
    limit: int = Query(5, title="Number of results to return", lt=MAX_LIMIT),
    fuzzy: bool = Query(False, title="Enable n-gram matching of partial names"),
    nested: bool = Query(False, title="Include adjacent entities in response"),
):
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting a batch of entities, each
    formatted like those returned by the API.

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
            "results": [...]
        },
        "entity2": {
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
    tasks = []
    for name, example in query.get("queries").items():
        tasks.append(_match_one(name, ds, example, fuzzy, limit, nested))
    if not len(tasks):
        raise HTTPException(400, "No queries provided.")
    responses = await asyncio.gather(*tasks)
    return {"responses": {n: r for n, r in responses}}


@app.get("/entities/{entity_id}", tags=["Data access"], response_model=EntityResponse)
async def fetch_entity(
    entity_id: str = Path(None, description="ID of the entity to retrieve")
):
    """Retrieve a single entity by its ID. The entity will be returned in
    full, with data from all datasets and with nested entities (adjacent
    passport, sanction and associated entities) included."""
    try:
        entity = await get_entity(entity_id)
    except EntityRedirect as redir:
        url = app.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    scope = await get_scope()
    data = await serialize_entity(scope, entity, nested=True)
    return data


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
    limit: int = Query(50, title="Number of results to return"),
    offset: int = Query(0, title="Number of results to skip before returning them"),
):
    """Access raw entity data as statements. OpenSanctions stores all of its
    material as a set of statements, e.g. 'the US sanctions list, as of our
    most recent update, claims that entity X has the property `name` set to
    the value `John Doe`'.

    All other forms of the data, including the nested JSON format usually
    returned by this API, are derived from this format and simplified for
    easier use. Using the raw statement data offered by this API will grant
    users access to detailed provenance information for each property value.
    """
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
    return await statement_results(query, limit, offset)


@app.get(
    "/reconcile/{dataset}",
    summary="Reconciliation info",
    tags=["Reconciliation"],
    response_model=Union[FreebaseManifest, FreebaseQueryResult],
)
async def reconcile(
    queries: Optional[str] = None,
    dataset: str = PATH_DATASET,
):
    """Reconciliation API, emulates Google Refine API. This endpoint can be used
    to bulk match entities against the system using an end-user application like
    [OpenRefine](https://openrefine.org).
    """
    ds = await get_dataset(dataset)
    if queries is not None:
        return await reconcile_queries(ds, queries)
    base_url = urljoin(settings.ENDPOINT_URL, f"/reconcile/{dataset}")
    return {
        "versions": ["0.2"],
        "name": f"{ds.title} ({settings.TITLE})",
        "identifierSpace": "https://opensanctions.org/reference/#schema",
        "schemaSpace": "https://opensanctions.org/reference/#schema",
        "view": {"url": ("https://opensanctions.org/entities/{{id}}/")},
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
    limit = min(MAX_LIMIT, int(query.get("limit", 5)))
    type = query.get("type", settings.BASE_SCHEMA)
    proxy = EntityProxy.from_dict(model, {"schema": type})
    proxy.add("name", query.get("query"))
    # proxy.add("notes", query.get("query"))
    for p in query.get("properties", []):
        prop = model.get_qname(p.get("pid"))
        if prop is None:
            continue
        try:
            proxy.add_cast(prop.schema, prop.name, p.get("v"), fuzzy=True)
        except InvalidData:
            log.exception("Invalid property is set.")

    results = []
    # log.info("QUERY %r %s", proxy.to_dict(), limit)
    query = entity_query(dataset, proxy, fuzzy=True)
    async for result, score in query_entities(query, limit=limit):
        results.append(get_freebase_entity(result, score))
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
    limit: int = Query(10, description="Number of suggestions to return"),
):
    """Suggest an entity based on a text query. This is functionally very
    similar to the basic search API, but returns data in the structure assumed
    by the community specification.

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    ds = await get_dataset(dataset)
    entity = EntityProxy.from_dict(model, {"schema": settings.BASE_SCHEMA})
    entity.add("name", prefix)
    results = []
    query = entity_query(ds, entity)
    async for result, score in query_entities(query, limit=limit):
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
    ds = get_dataset(dataset)
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
    ds = get_dataset(dataset)
    matches = []
    for schema in await get_matchable_schemata():
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(get_freebase_type(schema))
    return {
        "prefix": prefix,
        "result": matches,
    }
