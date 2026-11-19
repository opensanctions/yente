import asyncio
from collections.abc import Coroutine, Sequence
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Response
from nomenklatura.matching.types import ScoringConfig

from yente import settings
from yente.data.common import (
    EntityExample,
    ErrorResponse,
    TotalSpec,
)
from yente.data.dataset import Dataset
from yente.data.match_v1 import EntityMatches, EntityMatchQuery, EntityMatchResponse
from yente.logs import get_logger
from yente.provider import SearchProvider
from yente.routers.match import (
    build_match_query,
    candidate_count,
    echo_example,
    get_algorithm,
    score_candidates,
)
from yente.routers.util import (
    ALGO_HELP,
    TS_PATTERN,
    DatasetPath,
    ProviderDep,
    get_dataset,
)
from yente.search.queries import Filters, FilterSpec

log = get_logger(__name__)
router = APIRouter()


async def _match_one_query(
    ds: Dataset,
    algorithm_name: str,
    match: EntityMatchQuery,
    name: str,
    example: EntityExample,
    filters: Filters,
    include_dataset: Sequence[str],
    exclude_schema: Sequence[str],
    exclude_dataset: Sequence[str],
    changed_since: str | None,
    exclude_entity_ids: Sequence[str],
    provider: SearchProvider,
    candidates: int,
    limit: int,
    cutoff: float,
    threshold: float,
) -> tuple[str, EntityMatches]:
    """Run a single match query and return the results. This run as one big async process per query in the batch,
    and all queries are triggered simultaneously. Previous yente versions used to do all searches concurrently
    but score sequentially - but this meant that scoring would not start before the slowest ES query returned.
    """
    entity, query = build_match_query(
        example=example,
        dataset_names=ds.dataset_names,
        filters=filters,
        include_dataset=include_dataset,
        exclude_schema=exclude_schema,
        exclude_dataset=exclude_dataset,
        changed_since=changed_since,
        exclude_entity_ids=exclude_entity_ids,
    )
    total, scored = await score_candidates(
        provider,
        entity,
        query,
        get_algorithm(algorithm_name),
        ScoringConfig(weights=match.weights, config=match.config),
        candidates=candidates,
        limit=limit,
        threshold=threshold,
        cutoff=cutoff,
        metric_attributes={"endpoint": "match"},
    )
    log.info(
        f"/match/{ds.name}",
        action="match",
        schema=entity.schema.name,
        results=total,
        threshold=threshold,
        dataset=ds.name,
        limit=limit,
        # Log the algorithm passed in the request, which may be None or "best"
        algorithm_param=algorithm_name,
    )
    matches = EntityMatches(
        status=200,
        results=scored,
        total=TotalSpec(value=total, relation="eq"),
        query=echo_example(example, entity),
    )
    return (name, matches)


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
    provider: ProviderDep,
    dataset: DatasetPath,
    limit: Annotated[
        int, Query(title="Number of results to return", le=settings.MAX_MATCHES)
    ] = settings.MATCH_PAGE,
    threshold: Annotated[
        float,
        Query(title="Score threshold for results to be considered matches"),
    ] = settings.SCORE_THRESHOLD,
    cutoff: Annotated[
        float,
        Query(
            deprecated=True,
            title="Deprecated, use `threshold` instead. Lower bound of score for results to be returned at all",
        ),
    ] = settings.SCORE_THRESHOLD,
    algorithm: Annotated[str, Query(title=ALGO_HELP)] = settings.DEFAULT_ALGORITHM,
    include_dataset: Annotated[
        tuple[str, ...],
        Query(title="Only include the given datasets in results"),
    ] = (),
    exclude_schema: Annotated[
        tuple[str, ...],
        Query(title="Remove the given types of entities from results"),
    ] = (),
    exclude_dataset: Annotated[
        tuple[str, ...],
        Query(title="Remove the given datasets from results"),
    ] = (),
    topics: Annotated[
        tuple[str, ...],
        Query(title="Only return results that match any of the given topics"),
    ] = (),
    changed_since: Annotated[
        str | None,
        Query(
            pattern=TS_PATTERN,
            title="Match against entities that were updated since the given date",
        ),
    ] = None,
    # This list applies to all queries, but really only makes sense with a single query.
    # Our API design currently doesn't support per-query options such as this one.
    exclude_entity_ids: Annotated[
        tuple[str, ...],
        Query(
            title="A list of entities IDs to exclude from matching",
            description="The entity IDs supplied here do not have to be canonical. Supplying any of the referents of a merged entity will exclude that entity. This parameter may be useful for example to exclude false-positive matches that have been decided upon by a human.",
            max_length=50,
        ),
    ] = (),
) -> EntityMatchResponse:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting an entity formatted like
    those returned by the API.

    Tutorials:
    * [Using the matching API](https://www.opensanctions.org/docs/api/matching/)
    * [Configuring the scoring system](https://www.opensanctions.org/docs/api/scoring/)

    For example, the following would be a valid query:

    ```json
    "queries": {
        "q": {
            "schema": "Person",
            "properties": {
                "name": ["John Doe"],
                "birthDate": ["1975-04-21"],
                "nationality": ["us"]
            }
        }
    }
    ```
    The value for `q` can be chosen freely to correlate the result on the
    client side when the request is returned. The response will be given for the
    submitted example like this:

    ```json
    "responses": {
        "q": {
            "query": {},
            "results": [...]
        }
    }
    ```

    While more than one query can theoretically be submitted per request,
    batching is discouraged for performance and scalability reasons.

    The precision of the results will be dependent on the amount of detail submitted
    with each example. The following properties are most helpful for particular types:

    * **Person**: ``name``, ``birthDate``, ``nationality``, ``idNumber``, ``address``
    * **Organization**: ``name``, ``country``, ``registrationNumber``, ``address``
    * **Company**: ``name``, ``jurisdiction``, ``registrationNumber``, ``address``,
      ``incorporationDate``
    """
    ds = await get_dataset(dataset)
    # TODO: Remove this once get rid of cutoff. For now, make sure that the cutoff
    # is not greater than the threshold because that would be weird.
    cutoff = min(cutoff, threshold)
    filters: list[FilterSpec] = [("topics", t) for t in topics]
    get_algorithm(algorithm, match.config)

    if len(match.queries) > settings.MAX_BATCH:
        msg = f"Too many queries in one batch (limit: {settings.MAX_BATCH})"
        raise HTTPException(400, detail=msg)

    candidates = candidate_count(limit)

    tasks: list[Coroutine[Any, Any, tuple[str, EntityMatches]]] = []
    for name, example in match.queries.items():
        if example is None:
            continue
        # TODO: add more arguments here for readability:
        coroutine = _match_one_query(
            ds,
            algorithm,
            match,
            name,
            example,
            filters,
            include_dataset,
            exclude_schema,
            exclude_dataset,
            changed_since,
            exclude_entity_ids,
            provider,
            candidates,
            limit,
            cutoff,
            threshold,
        )
        tasks.append(coroutine)
    task_results = await asyncio.gather(*tasks)
    responses: dict[str, EntityMatches] = {}
    for name, matches in task_results:
        responses[name] = matches

    if not len(responses):
        raise HTTPException(400, detail="No queries provided.")

    response.headers["x-batch-size"] = str(len(responses))
    return EntityMatchResponse(
        responses=responses,
        limit=limit,
    )
