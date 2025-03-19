import asyncio
from typing import List, Optional, Union
from fastapi import APIRouter, Depends, Path, Query, Response, HTTPException
from fastapi.responses import RedirectResponse
from followthemoney import model
from followthemoney.types import registry
import enum

from yente import settings
from yente.logs import get_logger
from yente.data.common import (
    EntityAdjacentResponse,
    ErrorResponse,
    PropAdjacentResponse,
)
from yente.data.common import EntityResponse, SearchResponse
from yente.provider import SearchProvider, get_provider
from yente.search.queries import parse_sorts, text_query
from yente.search.queries import facet_aggregations
from yente.search.queries import FilterDict, Operator
from yente.search.search import get_entity, search_entities
from yente.search.search import result_entities, result_facets, result_total
from yente.search.nested import get_adjacent_prop, get_adjacents, serialize_entity
from yente.data import get_catalog
from yente.util import limit_window, EntityRedirect
from yente.routers.util import get_dataset
from yente.routers.util import PATH_DATASET, TS_PATTERN

log = get_logger(__name__)
router = APIRouter()


class Facet(str, enum.Enum):
    DATASETS = "datasets"
    SCHEMA = "schema"
    COUNTRIES = "countries"
    IDENTIFIERS = "identifiers"
    TOPICS = "topics"
    GENDERS = "genders"


DEFAULT_FACETS = [Facet.COUNTRIES, Facet.TOPICS, Facet.DATASETS]


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
    include_dataset: List[str] = Query(
        [],
        title="Restrict the search scope to datasets (that are in the given scope) to search entities within.",
        description="Limit the results to entities that are part of at least one of the given datasets.",
    ),
    exclude_dataset: List[str] = Query(
        [],
        title="Remove specific datasets (that are in the given scope) from the search scope.",
    ),
    exclude_schema: List[str] = Query(
        [], title="Remove the given types of entities from results"
    ),
    changed_since: Optional[str] = Query(
        None,
        pattern=TS_PATTERN,
        title="Search entities that were updated since the given date",
    ),
    countries: List[str] = Query([], title="Filter by country codes"),
    topics: List[str] = Query(
        [], title="Filter by entity topics (e.g. sanction, role.pep)"
    ),
    datasets: List[str] = Query(
        [],
        title="Filter by dataset names, for faceting use (respects operator choice).",
    ),
    limit: int = Query(
        settings.DEFAULT_PAGE, title="Number of results to return", le=settings.MAX_PAGE
    ),
    offset: int = Query(
        0, title="Start at result with given offset", le=settings.MAX_OFFSET
    ),
    sort: List[str] = Query([], title="Sorting criteria"),
    target: Optional[bool] = Query(
        None,
        title="Include only targeted entities",
        description="Please specify a list of topics of concern, instead.",
        deprecated=True,
    ),
    fuzzy: bool = Query(False, title="Allow fuzzy query syntax"),
    simple: bool = Query(False, title="Use simple syntax for user-facing query boxes"),
    facets: List[Facet] = Query(
        DEFAULT_FACETS,
        title="Facet counts to include in response.",
    ),
    filter_op: Operator = Query(
        "OR",
        title="Define behaviour of multiple filters on one field",
        description="Logic to use when combining multiple filters on the same field (topics, countries, datasets). Please specify AND for new integrations (to override a legacy default) and when building a faceted user interface.",
    ),
    provider: SearchProvider = Depends(get_provider),
) -> SearchResponse:
    """Search endpoint for matching entities based on a simple piece of text, e.g.
    a name. This can be used to implement a simple, user-facing search. For proper
    entity matching, the multi-property matching API should be used instead.

    Search queries can include field-specific fitlers, wildcards and fuzzy searches.
    See also: [search API documentation](https://www.opensanctions.org/docs/api/search/).
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
    query = text_query(
        ds,
        schema_obj,
        q,
        filters=filters,
        fuzzy=fuzzy,
        simple=simple,
        include_dataset=include_dataset,
        exclude_dataset=exclude_dataset,
        exclude_schema=exclude_schema,
        changed_since=changed_since,
        filter_op=filter_op,
    )
    aggregations = facet_aggregations([f.value for f in facets])
    resp = await search_entities(
        provider,
        query,
        limit=limit,
        offset=offset,
        aggregations=aggregations,
        sort=parse_sorts(sort),
    )
    results: List[EntityResponse] = []
    for result in result_entities(resp):
        results.append(await serialize_entity(provider, result))
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
    return output


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
    entity_id: str = Path(
        description="ID of the entity to retrieve", examples=["Q7747"]
    ),
    nested: bool = Query(
        True,
        title="Include adjacent entities (e.g. addresses, family) in response",
    ),
    provider: SearchProvider = Depends(get_provider),
) -> Union[RedirectResponse, EntityResponse]:
    """Retrieve a single entity by its ID. The entity will be returned in
    full, with data from all datasets and with nested entities (adjacent
    passport, sanction and associated entities) included. If the entity ID
    has been merged into a different canonical entity, an HTTP redirect will
    be triggered.

    Intro: [entity data model](https://www.opensanctions.org/docs/entities/).
    """
    try:
        entity = await get_entity(provider, entity_id)
    except EntityRedirect as redir:
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(status_code=308, url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    data = await serialize_entity(provider, entity, nested=nested)
    log.info(f"Fetch {data.id} [{data.schema_}]", action="entity", entity_id=entity_id)
    return data


@router.get(
    "/entities/{entity_id}/adjacent",
    tags=["Data access"],
    response_model=EntityAdjacentResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_entity(
    response: Response,
    entity_id: str = Path(
        description="ID of the entity to retrieve", examples=["Q7747"]
    ),
    provider: SearchProvider = Depends(get_provider),
    sort: List[str] = Query([], title="Sorting criteria"),
    limit: int = Query(
        settings.DEFAULT_PAGE,
        title="Number of results per property to return",
        le=settings.MAX_PAGE,
    ),
    offset: int = Query(
        0, title="Start at result with given offset", le=settings.MAX_OFFSET
    ),
) -> Union[RedirectResponse, EntityAdjacentResponse]:
    try:
        entity = await get_entity(provider, entity_id)
    except EntityRedirect as redir:
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(status_code=308, url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")

    log.info(
        f"Fetch {entity.id} [{entity.schema.name}]",
        action="adjacent",
        entity_id=entity_id,
    )
    return await get_adjacents(
        provider,
        entity,
        limit=limit,
        offset=offset,
        sort=parse_sorts(sort, default="_doc"),
    )


@router.get(
    "/entities/{entity_id}/adjacent/{prop_name}",
    tags=["Data access"],
    response_model=PropAdjacentResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_entity(
    response: Response,
    entity_id: str = Path(
        description="ID of the entity to retrieve", examples=["Q7747"]
    ),
    prop_name: str = Path(
        description="Name of the property to fetch adjacent entities for",
        examples=["address", "ownershipOwner"],
    ),
    provider: SearchProvider = Depends(get_provider),
    sort: List[str] = Query([], title="Sorting criteria"),
    limit: int = Query(
        settings.DEFAULT_PAGE,
        title="Number of results per property to return",
        le=settings.MAX_PAGE,
    ),
    offset: int = Query(
        0, title="Start at result with given offset", le=settings.MAX_OFFSET
    ),
) -> Union[RedirectResponse, PropAdjacentResponse]:
    try:
        entity = await get_entity(provider, entity_id)
    except EntityRedirect as redir:
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(status_code=308, url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    if prop_name not in entity.schema.properties or entity.schema.properties[prop_name].type != registry.entity:
        raise HTTPException(404, detail="No such property!")

    log.info(
        f"Fetch {entity.id} [{entity.schema.name}:{prop_name}]",
        action="adjacent_prop",
        entity_id=entity_id,
        prop_name=prop_name,
    )
    return await get_adjacent_prop(
        provider,
        entity,
        entity.schema.properties[prop_name],
        limit=limit,
        offset=offset,
        sort=parse_sorts(sort, default="_doc"),
    )
