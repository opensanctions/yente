import asyncio
import structlog
from structlog.stdlib import BoundLogger
from typing import Any, Dict, List, Tuple
from fastapi import APIRouter, Path, Query
from fastapi import HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from followthemoney import model

from yente import settings
from yente.entity import Dataset
from yente.models import EntityExample
from yente.models import EntityMatchQuery, EntityMatchResponse
from yente.models import EntityResponse, SearchResponse
from yente.search.queries import text_query, entity_query
from yente.search.queries import facet_aggregations
from yente.search.search import get_entity, query_results
from yente.search.search import serialize_entity
from yente.util import limit_window, EntityRedirect
from yente.routers.util import get_dataset
from yente.routers.util import MATCH_PAGE, PATH_DATASET

log: BoundLogger = structlog.get_logger(__name__)
router = APIRouter()


@router.get(
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


@router.post(
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


@router.get(
    "/entities/{entity_id}", tags=["Data access"], response_model=EntityResponse
)
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
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    data = await serialize_entity(entity, nested=True)
    log.info(data.get("caption"), action="entity", entity_id=entity_id)
    return JSONResponse(content=data, headers=settings.CACHE_HEADERS)
