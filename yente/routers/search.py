import asyncio
from typing import Dict, List, Optional, Union
from fastapi import APIRouter, Path, Query, Response, HTTPException
from fastapi.responses import RedirectResponse
from nomenklatura.matching import explain_matcher
from followthemoney import model

from yente import settings
from yente.logs import get_logger
from yente.data.common import ErrorResponse
from yente.data.common import EntityMatchQuery, EntityMatchResponse
from yente.data.common import EntityResponse, SearchResponse, EntityMatches
from yente.search.queries import parse_sorts, text_query, entity_query
from yente.search.queries import facet_aggregations
from yente.search.queries import FilterDict
from yente.search.search import get_entity, search_entities
from yente.search.search import result_entities, result_facets, result_total
from yente.search.nested import serialize_entity
from yente.data import get_catalog
from yente.data.entity import Entity
from yente.util import limit_window, EntityRedirect
from yente.scoring import score_results
from yente.routers.util import get_dataset
from yente.routers.util import PATH_DATASET

log = get_logger(__name__)
router = APIRouter()


@router.get(
    "/search/{dataset}",
    summary="Simple entity search",
    tags=["Matching"],
    response_model=SearchResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def search(
    response: Response,
    q: str = Query("", title="Query text"),
    dataset: str = PATH_DATASET,
    schema: str = Query(
        settings.BASE_SCHEMA, title="Types of entities that can match the search"
    ),
    countries: List[str] = Query([], title="Filter by country codes"),
    topics: List[str] = Query(
        [], title="Filter by entity topics (e.g. sanction, role.pep)"
    ),
    datasets: List[str] = Query([], title="Filter by data sources"),
    limit: int = Query(10, title="Number of results to return", le=settings.MAX_PAGE),
    offset: int = Query(
        0, title="Start at result with given offset", le=settings.MAX_OFFSET
    ),
    sort: List[str] = Query([], title="Sorting criteria"),
    target: Optional[bool] = Query(None, title="Include only targeted entities"),
    fuzzy: bool = Query(False, title="Allow fuzzy query syntax"),
    simple: bool = Query(False, title="Use simple syntax for user-facing query boxes"),
) -> SearchResponse:
    """Search endpoint for matching entities based on a simple piece of text, e.g.
    a name. This can be used to implement a simple, user-facing search. For proper
    entity matching, the multi-property matching API should be used instead.

    Search queries can use the [ElasticSearch Query string syntax](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax)
    to perform field-specific searches, wildcard and fuzzy searches.
    """
    limit, offset = limit_window(limit, offset, 10)
    ds = await get_dataset(dataset)
    catalog = await get_catalog()
    schema_obj = model.get(schema)
    if schema_obj is None:
        raise HTTPException(400, detail="Invalid schema")
    filters: FilterDict = {
        "countries": countries,
        "topics": topics,
        "datasets": datasets,
    }
    if target is not None:
        filters["target"] = target
    query = text_query(ds, schema_obj, q, filters=filters, fuzzy=fuzzy, simple=simple)
    aggregations = facet_aggregations([f for f in filters.keys()])
    resp = await search_entities(
        query,
        limit=limit,
        offset=offset,
        aggregations=aggregations,
        sort=parse_sorts(sort),
    )
    results: List[EntityResponse] = []
    for result in result_entities(resp):
        results.append(await serialize_entity(result))
    output = SearchResponse(
        results=results,
        facets=result_facets(resp, catalog),
        total=result_total(resp),
        limit=limit,
        offset=offset,
    )
    log.info(
        f"/search/{ds.name}",
        action="search",
        length=len(q),
        dataset=ds.name,
        results=output.total.value,
    )
    response.headers.update(settings.CACHE_HEADERS)
    return output


@router.post(
    "/match/{dataset}",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatchResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def match(
    response: Response,
    match: EntityMatchQuery,
    dataset: str = PATH_DATASET,
    limit: int = Query(
        settings.MATCH_PAGE,
        title="Number of results to return",
        le=settings.MAX_MATCHES,
    ),
    threshold: float = Query(
        settings.SCORE_THRESHOLD,
        title="Score threshold for results to be considered matches",
    ),
    cutoff: float = Query(
        settings.SCORE_CUTOFF,
        title="Lower bound of score for results to be returned at all",
    ),
) -> EntityMatchResponse:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting a batch of entities, each
    formatted like those returned by the API.

    Tutorial: [Using the matching API to do KYC-style checks](https://www.opensanctions.org/articles/2022-02-01-matching-api/).

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
    limit, _ = limit_window(limit, 0, settings.MATCH_PAGE)

    if len(match.queries) > settings.MAX_BATCH:
        msg = "Too many queries in one batch (limit: %d)" % settings.MAX_BATCH
        raise HTTPException(400, detail=msg)

    queries = []
    entities = []
    responses: Dict[str, EntityMatches] = {}

    for name, example in match.queries.items():
        try:
            entity = Entity.from_example(example.schema_, example.properties)
            query = entity_query(ds, entity)
        except Exception as exc:
            log.info("Cannot parse example entity: %s" % str(exc))
            raise HTTPException(
                status_code=400,
                detail=f"Cannot parse example entity: {exc}",
            )
        queries.append(search_entities(query, limit=limit * 3))
        entities.append((name, entity))
    if not len(queries) and not len(responses):
        raise HTTPException(400, detail="No queries provided.")
    results = await asyncio.gather(*queries)

    for (name, entity), resp in zip(entities, results):
        ents = result_entities(resp)
        scored = score_results(
            entity, ents, threshold=threshold, cutoff=cutoff, limit=limit
        )
        total = result_total(resp)
        log.info(
            f"/match/{ds.name}",
            action="match",
            schema=entity.schema.name,
            results=total.value,
        )
        responses[name] = EntityMatches(
            status=200,
            results=scored,
            total=total,
            query=entity.to_dict(),
        )
    matcher = explain_matcher()
    response.headers["x-batch-size"] = str(len(responses))
    return EntityMatchResponse(responses=responses, matcher=matcher, limit=limit)


@router.get(
    "/entities/{entity_id}",
    tags=["Data access"],
    response_model=EntityResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_entity(
    response: Response,
    entity_id: str = Path(None, description="ID of the entity to retrieve"),
    nested: bool = Query(
        True,
        title="Include adjacent entities (e.g. addresses, family, subsidiaries) in response",
    ),
) -> Union[RedirectResponse, EntityResponse]:
    """Retrieve a single entity by its ID. The entity will be returned in
    full, with data from all datasets and with nested entities (adjacent
    passport, sanction and associated entities) included. If the entity ID
    has been merged into a different canonical entity, an HTTP redirect will
    be triggered.

    Intro: [entity data model](https://www.opensanctions.org/docs/entities/).
    """
    try:
        entity = await get_entity(entity_id)
    except EntityRedirect as redir:
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(status_code=308, url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    data = await serialize_entity(entity, nested=nested)
    log.info(data.caption, action="entity", entity_id=entity_id)
    response.headers.update(settings.CACHE_HEADERS)
    return data
