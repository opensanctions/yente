import enum
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query, Response
from fastapi.responses import RedirectResponse
from followthemoney import model
from followthemoney.types import registry

from yente import settings
from yente.data import get_catalog
from yente.data.common import (
    AdjacentResultsResponse,
    EntityAdjacentResponse,
    EntityResponse,
    ErrorResponse,
    SearchResponse,
)
from yente.logs import get_logger
from yente.routers.util import TS_PATTERN, DatasetPath, ProviderDep, get_dataset
from yente.search.nested import get_adjacent_entities, get_nested_entity
from yente.search.queries import (
    FilterSpec,
    Operator,
    facet_aggregations,
    parse_sorts,
    text_query,
)
from yente.search.search import (
    get_entity,
    result_entities,
    result_facets,
    result_total,
    search_entities,
    upscore_large_entities,
)
from yente.util import EntityRedirect, limit_window

log = get_logger(__name__)
router = APIRouter()


class Facet(enum.StrEnum):
    DATASETS = "datasets"
    SCHEMA = "schema"
    COUNTRIES = "countries"
    IDENTIFIERS = "identifiers"
    TOPICS = "topics"
    GENDERS = "genders"


DEFAULT_FACETS = (Facet.COUNTRIES, Facet.TOPICS, Facet.DATASETS)


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
    provider: ProviderDep,
    dataset: DatasetPath,
    q: Annotated[str, Query(title="Query text")] = "",
    schema: Annotated[
        str, Query(title="Types of entities that can match the search")
    ] = settings.BASE_SCHEMA,
    include_dataset: Annotated[
        tuple[str, ...],
        Query(
            title="Restrict the search scope to datasets (that are in the given scope) to search entities within.",
            description="Limit the results to entities that are part of at least one of the given datasets.",
        ),
    ] = (),
    exclude_dataset: Annotated[
        tuple[str, ...],
        Query(
            title="Remove specific datasets (that are in the given scope) from the search scope.",
        ),
    ] = (),
    exclude_schema: Annotated[
        tuple[str, ...],
        Query(title="Remove the given types of entities from results"),
    ] = (),
    changed_since: Annotated[
        str | None,
        Query(
            pattern=TS_PATTERN,
            title="Search entities that were updated since the given date",
        ),
    ] = None,
    countries: Annotated[tuple[str, ...], Query(title="Filter by country codes")] = (),
    topics: Annotated[
        tuple[str, ...],
        Query(title="Filter by entity topics (e.g. sanction, role.pep)"),
    ] = (),
    datasets: Annotated[
        tuple[str, ...],
        Query(
            title="Filter by dataset names, for faceting use (respects operator choice).",
        ),
    ] = (),
    filter: Annotated[
        tuple[str, ...],
        Query(
            title="Filter by entity properties (e.g. programId, birthDate)",
            description="Use the syntax `field:value` to filter on a specific field. Properties are indexed as fields named `properties.birthDate:1985`.",
        ),
    ] = (),
    limit: Annotated[
        int, Query(title="Number of results to return", le=settings.MAX_PAGE)
    ] = settings.DEFAULT_PAGE,
    offset: Annotated[
        int, Query(title="Start at result with given offset", le=settings.MAX_OFFSET)
    ] = 0,
    sort: Annotated[tuple[str, ...], Query(title="Sorting criteria")] = (),
    target: Annotated[
        bool | None,
        Query(
            title="Include only targeted entities",
            description="Please specify a list of topics of concern, instead.",
            deprecated=True,
        ),
    ] = None,
    fuzzy: Annotated[bool, Query(title="Allow fuzzy query syntax")] = False,
    simple: Annotated[
        bool, Query(title="Use simple syntax for user-facing query boxes")
    ] = False,
    facets: Annotated[
        tuple[Facet, ...],
        Query(title="Facet counts to include in response."),
    ] = DEFAULT_FACETS,
    filter_op: Annotated[
        Operator,
        Query(
            title="Define behaviour of multiple filters on one field",
            description="Logic to use when combining multiple filters on the same field (topics, countries, datasets). Please specify AND for new integrations (to override a legacy default) and when building a faceted user interface.",
        ),
    ] = Operator.OR,
) -> SearchResponse:
    """Search endpoint for matching entities based on a simple piece of text, e.g.
    a name. This can be used to implement a simple, user-facing search. For proper
    entity matching, the multi-property matching API should be used instead.

    Search queries can include field-specific fitlers, wildcards and fuzzy searches.
    See also: [search API documentation](https://www.opensanctions.org/docs/api/search/).
    """
    limit, offset = limit_window(limit, offset)
    ds = await get_dataset(dataset)
    catalog = await get_catalog()
    schema_obj = model.get(schema)
    if schema_obj is None:
        raise HTTPException(400, detail="Invalid schema")

    filters: list[FilterSpec] = [("countries", c) for c in countries]
    filters.extend([("topics", t) for t in topics])
    filters.extend([("datasets", d) for d in datasets])
    for flt in filter:
        try:
            field, value = flt.split(":", 1)
            filters.append((field, value))
        except ValueError:
            raise HTTPException(400, detail=f"Invalid filter: {flt!r}")
    if target is not None:
        filters.append(("target", target))
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
    query = upscore_large_entities(query)
    resp = await search_entities(
        provider,
        query,
        limit=limit,
        offset=offset,
        aggregations=aggregations,
        sort=parse_sorts(sort),
    )
    results: list[EntityResponse] = []
    for result, _ in result_entities(resp):
        results.append(EntityResponse.from_entity(result))
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
        308: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_entity(
    response: Response,
    provider: ProviderDep,
    entity_id: Annotated[
        str, Path(description="ID of the entity to retrieve", examples=["Q7747"])
    ],
    nested: Annotated[
        bool,
        Query(title="Include adjacent entities (e.g. addresses, family) in response"),
    ] = True,
) -> RedirectResponse | EntityResponse:
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
    if nested:
        data, _total = await get_nested_entity(provider, entity)
    else:
        data = EntityResponse.from_entity(entity)
    log.info(f"Fetch {data.id} [{data.schema_}]", action="entity", entity_id=entity_id)
    return data


@router.get(
    "/entities/{entity_id}/adjacent",
    name="Fetch Adjacent Entities*",
    tags=["Data access"],
    response_model=EntityAdjacentResponse,
    responses={
        308: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_adjacent_entities(
    response: Response,
    provider: ProviderDep,
    entity_id: Annotated[
        str,
        Path(
            description="ID of the entity whose graph context was requested",
            examples=["Q7747"],
        ),
    ],
    sort: Annotated[tuple[str, ...], Query(title="Sorting criteria")] = (),
    limit: Annotated[
        int,
        Query(title="Number of results per property to return", le=settings.MAX_PAGE),
    ] = settings.DEFAULT_PAGE,
    offset: Annotated[
        int, Query(title="Start at result with given offset", le=settings.MAX_OFFSET)
    ] = 0,
) -> RedirectResponse | EntityAdjacentResponse:
    """***Beta:** This endpoint is released for wider testing and is not yet recommended
    for production use. We welcome feedback. Its interface may change without announcement.

    Retrieve entities adjacent to a given entity e.g. passports, sanctions, associates.

    This endpoint offers the same information as adjacent entities nested in
    [`/entities/{entity_id}`](#tag/Data-access/operation/fetch_entity_entities__entity_id__get),
    but offers pagination for cases where the number of results is potentially very large.
    """
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
    return await get_adjacent_entities(
        provider,
        entity,
        limit=limit,
        offset=offset,
        sort=parse_sorts(sort, defaults=["_doc"]),
    )


@router.get(
    "/entities/{entity_id}/adjacent/{property_name}",
    tags=["Data access"],
    name="Fetch Adjacent by Property*",
    response_model=AdjacentResultsResponse,
    responses={
        308: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity or property not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def fetch_adjacent_by_prop(
    response: Response,
    provider: ProviderDep,
    entity_id: Annotated[
        str,
        Path(
            description="ID of the entity hose graph context was requested",
            examples=["Q7747"],
        ),
    ],
    property_name: Annotated[
        str,
        Path(
            description="Name of the property to fetch adjacent entities for",
            examples=["address", "ownershipOwner"],
        ),
    ],
    sort: Annotated[tuple[str, ...], Query(title="Sorting criteria")] = (),
    limit: Annotated[
        int,
        Query(title="Number of results per property to return", le=settings.MAX_PAGE),
    ] = settings.DEFAULT_PAGE,
    offset: Annotated[
        int, Query(title="Start at result with given offset", le=settings.MAX_OFFSET)
    ] = 0,
) -> RedirectResponse | AdjacentResultsResponse:
    """***Beta:** This endpoint is released for wider testing and is not yet recommended
    for production use. We welcome feedback. Its interface may change without announcement.

    Retrieve entities adjacent to a given entity for a specific property.

    This endpoint offers the same information as adjacent entities nested in
    [`/entities/{entity_id}`](#tag/Data-access/operation/fetch_entity_entities__entity_id__get),
    but offers pagination for cases where the number of results is potentially very large.
    """
    try:
        entity = await get_entity(provider, entity_id)
    except EntityRedirect as redir:
        url = router.url_path_for("fetch_entity", entity_id=redir.canonical_id)
        return RedirectResponse(status_code=308, url=url)
    if entity is None:
        raise HTTPException(404, detail="No such entity!")
    if (
        property_name not in entity.schema.properties
        or entity.schema.properties[property_name].type != registry.entity
    ):
        raise HTTPException(404, detail="No such property!")

    log.info(
        f"Fetch {entity.id} [{entity.schema.name}:{property_name}]",
        action="adjacent_prop",
        entity_id=entity_id,
        property_name=property_name,
    )
    prop = entity.schema.properties[property_name]
    nested, total = await get_nested_entity(
        provider,
        entity,
        prop,
        parse_sorts(sort, defaults=["_doc"]),
        limit,
        offset,
    )
    results = nested.properties.get(prop.name, [])
    return AdjacentResultsResponse(
        results=results,
        total=total,
        limit=limit,
        offset=offset,
    )
