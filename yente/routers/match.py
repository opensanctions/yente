import asyncio
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, Query, Response, HTTPException

from yente import settings
from yente.logs import get_logger
from yente.data.common import ErrorResponse
from yente.data.common import EntityMatchQuery, EntityMatchResponse, EntityExample
from yente.data.common import EntityMatches, TotalSpec
from yente.provider import SearchProvider, get_provider
from yente.search.queries import entity_query, FilterDict
from yente.search.search import search_entities, result_entities
from yente.data.entity import Entity
from yente.util import limit_window
from yente.scoring import score_results
from yente.routers.util import get_dataset, get_algorithm_by_name
from yente.routers.util import PATH_DATASET, TS_PATTERN, ALGO_HELP

log = get_logger(__name__)
router = APIRouter()


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
    algorithm: str = Query(settings.DEFAULT_ALGORITHM, title=ALGO_HELP),
    include_dataset: List[str] = Query(
        [], title="Only include the given datasets in results"
    ),
    exclude_schema: List[str] = Query(
        [], title="Remove the given types of entities from results"
    ),
    exclude_dataset: List[str] = Query(
        [], title="Remove the given datasets from results"
    ),
    topics: List[str] = Query(
        [], title="Only return results that match the given topics"
    ),
    changed_since: Optional[str] = Query(
        None,
        pattern=TS_PATTERN,
        title="Match against entities that were updated since the given date",
    ),
    provider: SearchProvider = Depends(get_provider),
) -> EntityMatchResponse:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting a batch of entities, each
    formatted like those returned by the API.

    Tutorials:
    * [Using the matching API](https://www.opensanctions.org/docs/api/matching/)
    * [Configuring the scoring system](https://www.opensanctions.org/docs/api/scoring/)

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
    algorithm_type = get_algorithm_by_name(algorithm)

    if len(match.queries) > settings.MAX_BATCH:
        msg = "Too many queries in one batch (limit: %d)" % settings.MAX_BATCH
        raise HTTPException(400, detail=msg)

    filters: FilterDict = {"topics": topics}
    queries = []
    entities = []
    responses: Dict[str, EntityMatches] = {}

    for name, example in match.queries.items():
        if example is None:
            continue
        try:
            entity = Entity.from_example(example)
            query = entity_query(
                ds,
                entity,
                filters=filters,
                include_dataset=include_dataset,
                exclude_schema=exclude_schema,
                exclude_dataset=exclude_dataset,
                changed_since=changed_since,
            )
        except Exception as exc:
            log.info("Cannot parse example entity: %s" % str(exc))
            raise HTTPException(
                status_code=400,
                detail=f"Cannot parse example entity: {exc}",
            )
        # We're using a higher limit for candidate generation, because we want to
        # get a broad range of candidates to score against. This is a trade-off
        # between speed and accuracy.
        candidates = limit * settings.MATCH_CANDIDATES
        candidates = max(20, min(settings.MAX_RESULTS, candidates))
        queries.append(search_entities(provider, query, limit=candidates))
        entities.append((name, entity))
    if not len(queries) and not len(responses):
        raise HTTPException(400, detail="No queries provided.")
    results = await asyncio.gather(*queries)

    for (name, entity), resp in zip(entities, results):
        ents = result_entities(resp)
        total, scored = score_results(
            algorithm_type,
            entity,
            ents,
            threshold=threshold,
            cutoff=cutoff,
            limit=limit,
            weights=match.weights,
        )
        log.info(
            f"/match/{ds.name}",
            action="match",
            schema=entity.schema.name,
            results=total,
        )
        responses[name] = EntityMatches(
            status=200,
            results=scored,
            total=TotalSpec(value=total, relation="eq"),
            query=EntityExample.model_validate(entity.to_dict()),
        )
    response.headers["x-batch-size"] = str(len(responses))
    return EntityMatchResponse(
        responses=responses,
        matcher=algorithm_type.explain(),
        limit=limit,
    )
