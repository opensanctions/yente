from fastapi import APIRouter, HTTPException
from followthemoney.dataset import evaluate_query
from followthemoney.exc import InvalidDatasetQuery
from nomenklatura.matching.types import ScoringConfig

from yente.data import get_catalog
from yente.data.common import ErrorResponse, TotalSpec
from yente.data.match_v2 import EntityMatches, EntityMatchQuery
from yente.logs import get_logger
from yente.routers.match import (
    build_match_query,
    candidate_count,
    echo_example,
    get_algorithm,
    score_candidates,
)
from yente.routers.util import ProviderDep
from yente.search.queries import FilterSpec

log = get_logger(__name__)
router = APIRouter(prefix="/v2")


@router.post(
    "/match",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatches,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def match(
    provider: ProviderDep,
    match: EntityMatchQuery,
) -> EntityMatches:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting an entity formatted like
    those returned by the API.

    Tutorials:
    * [Using the matching API](https://www.opensanctions.org/docs/api/matching/)
    * [Configuring the scoring system](https://www.opensanctions.org/docs/api/scoring/)

    Each request matches a single entity. For example, the following would be a
    valid request:

    ```json
    {
        "query": {
            "schema": "Person",
            "properties": {
                "name": ["John Doe"],
                "birthDate": ["1975-04-21"],
                "nationality": ["us"]
            }
        },
        "filters": {
            "datasets": ["#list.sanction"]
        }
    }
    ```

    The datasets to match against are selected using a
    [dataset query](https://followthemoney.tech/docs/metadata/#dataset-query-dsl):
    a list of dataset names, collection names and `#tag` selectors, or a
    combination of them using `or`, `and` and `not`, e.g.
    `{"and": ["default", {"not": "#list.pep"}]}`. If no datasets are given, the
    `default` collection is used.

    The response contains the scored results and the query entity as it was
    understood by the matcher:

    ```json
    {
        "query": {},
        "results": [...],
        "total": {"value": 1, "relation": "eq"}
    }
    ```

    The precision of the results will be dependent on the amount of detail submitted
    with each example. The following properties are most helpful for particular types:

    * **Person**: ``name``, ``birthDate``, ``nationality``, ``idNumber``, ``address``
    * **Organization**: ``name``, ``country``, ``registrationNumber``, ``address``
    * **Company**: ``name``, ``jurisdiction``, ``registrationNumber``, ``address``,
      ``incorporationDate``
    """
    filters = match.filters
    catalog = await get_catalog()
    try:
        scope = evaluate_query(catalog, filters.datasets)
    except InvalidDatasetQuery as exc:
        raise HTTPException(400, detail=f"Invalid dataset query: {exc}")
    algorithm = get_algorithm(match.algorithm, match.matcher.config)
    # A cutoff above the threshold would hide matches.
    cutoff = min(match.cutoff, match.threshold)
    topics: list[FilterSpec] = [("topics", t) for t in filters.topics]

    entity, query = build_match_query(
        example=match.query,
        dataset_names={ds.name for ds in scope},
        filters=topics,
        exclude_schema=filters.exclude_schemata,
        changed_since=filters.changed_since,
        exclude_entity_ids=filters.exclude_entities,
    )
    total, scored = await score_candidates(
        provider,
        entity,
        query,
        algorithm,
        ScoringConfig(weights=match.matcher.weights, config=match.matcher.config),
        candidates=candidate_count(match.limit),
        limit=match.limit,
        threshold=match.threshold,
        cutoff=cutoff,
        metric_attributes={"endpoint": "match_v2"},
    )
    log.info(
        "/v2/match",
        action="match",
        schema=entity.schema.name,
        results=total,
        threshold=match.threshold,
        datasets=filters.datasets,
        limit=match.limit,
        # Log the algorithm passed in the request, which may be "best"
        algorithm_param=match.algorithm,
    )
    return EntityMatches(
        results=scored,
        total=TotalSpec(value=total, relation="eq"),
        query=echo_example(match.query, entity),
    )
