from collections.abc import Collection, Mapping, Sequence

from fastapi import HTTPException
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig
from opentelemetry.util.types import AttributeValue

from yente import settings
from yente.data.common import EntityExample, ScoredEntityResponse
from yente.data.entity import Entity
from yente.logs import get_logger
from yente.provider import SearchProvider
from yente.routers.util import get_algorithm_by_name
from yente.scoring import score_results
from yente.search.queries import DEFAULT_SORTS, Clause, Filters, Operator, entity_query
from yente.search.search import result_entities, search_entities
from yente.util import limit_window

log = get_logger(__name__)


def get_algorithm(name: str, config: Collection[str] = ()) -> type[ScoringAlgorithm]:
    """Look up a scoring algorithm by name and check that the given configuration
    keys are supported by it."""
    algorithm = get_algorithm_by_name(name)
    for config_key in config:
        if config_key not in algorithm.CONFIG:
            raise HTTPException(
                400,
                detail=f"Invalid configuration parameter: {config_key} for algorithm {algorithm.NAME}",
            )
    return algorithm


def candidate_count(limit: int) -> int:
    """Number of candidates to retrieve from the index for scoring, given the
    number of results to return."""
    # We're using a higher limit for candidate generation, because we want to
    # get a broad range of candidates to score against. This is a trade-off
    # between speed and accuracy.
    candidates = limit * settings.MATCH_CANDIDATES
    # We want at least 20 candidates to score to have good results even for limit = 1
    candidates = max(20, min(settings.MAX_RESULTS, candidates))
    # We want to retrieve at most MAX_MATCH_CANDIDATES, other wise we will be
    # very slow or OOM for high values of limit.
    # But at least `limit` candidates, otherwise we'll run out of results to return.
    # One day we might drop the max for limit, see https://github.com/opensanctions/yente/issues/927
    candidates = min(candidates, max(settings.MAX_MATCH_CANDIDATES, limit))
    # This is more of a formality - candidates will never be >= 10000 (settings.MAX_RESULTS)
    candidates, _ = limit_window(candidates, 0)
    return candidates


def build_match_query(
    example: EntityExample,
    dataset_names: Collection[str],
    filters: Filters,
    include_dataset: Sequence[str] = (),
    exclude_schema: Sequence[str] = (),
    exclude_dataset: Sequence[str] = (),
    changed_since: str | None = None,
    exclude_entity_ids: Sequence[str] = (),
) -> tuple[Entity, Clause]:
    """Parse the example into an entity and build the candidate query for it."""
    try:
        entity = Entity.from_example(example)
        query = entity_query(
            dataset_names,
            entity,
            filters=filters,
            filter_op=Operator.OR,
            include_dataset=include_dataset,
            exclude_schema=exclude_schema,
            exclude_dataset=exclude_dataset,
            changed_since=changed_since,
            exclude_entity_ids=exclude_entity_ids,
        )
    # Any failure to build a query from the example is the client's fault.
    except Exception as exc:  # noqa: BLE001
        log.info(f"Cannot parse example entity: {exc!s}")
        raise HTTPException(
            status_code=400,
            detail=f"Cannot parse example entity: {exc}",
        )
    return entity, query


async def score_candidates(
    provider: SearchProvider,
    entity: Entity,
    query: Clause,
    algorithm: type[ScoringAlgorithm],
    scoring_config: ScoringConfig,
    candidates: int,
    limit: int,
    threshold: float,
    cutoff: float,
    metric_attributes: Mapping[str, AttributeValue],
) -> tuple[int, list[ScoredEntityResponse]]:
    """Retrieve candidates for the query and score them against the entity."""
    # The response total is the number of scored results, so the candidate
    # query never reads hits.total; skipping the count lets the backend
    # prune non-competitive documents early.
    search_result = await search_entities(
        provider,
        query,
        limit=candidates,
        sort=DEFAULT_SORTS,
        track_total_hits=False,
        metric_attributes=metric_attributes,
    )
    ents = result_entities(search_result)
    return await score_results(
        algorithm,
        entity,
        ents,
        threshold=threshold,
        cutoff=cutoff,
        limit=limit,
        config=scoring_config,
    )


def echo_example(example: EntityExample, entity: Entity) -> EntityExample:
    """The parsed query entity as returned in the response."""
    # Restore the caller-supplied id on the echoed query only. The internal
    # entity keeps its checksum-derived id so scoring/candidate-dedup can't
    # be influenced by a caller picking an id that collides with a real
    # dataset entity.
    return EntityExample(
        id=example.id,
        schema=entity.schema.name,
        properties=dict(entity.properties),
    )
