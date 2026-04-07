import asyncio
from typing import Iterable, List, Type, Tuple
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse

log = get_logger(__name__)

# Early stopping: candidates from ES are scored one by one by the matching algorithm
# (e.g. LogicV2). In production, ~82% of scoring calls produce scores below cutoff, and
# ~49% of queries have zero candidates above 0.5. To avoid wasting CPU on hopeless
# candidates, we stop scoring after `patience` consecutive low-scoring results.
#
# When a promising score has been seen, patience is multiplied by EARLY_STOP_BOOST_FACTOR
# to keep searching — queries with real matches tend to have good results scattered across
# ES ranks (ES and algo scores correlate weakly).
#
# Thresholds are derived from the per-request `threshold` parameter so that users with
# lower thresholds automatically get less aggressive early stopping.
#
# Caveat: this can miss results buried deep in the ES ranking. In production log analysis
# (418 queries), the recommended defaults missed 5 results (1.2%), all sub-threshold
# (highest 0.667 vs 0.7 threshold). Set YENTE_SCORE_EARLY_STOP_PATIENCE high to disable.
EARLY_STOP_BOOST_FACTOR = 4


async def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Tuple[Entity, float]],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: int = settings.MATCH_PAGE,
    config: ScoringConfig = ScoringConfig.defaults(),
) -> Tuple[int, List[ScoredEntityResponse]]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    # Early stopping variables:
    consecutive_low = 0
    patience = settings.SCORE_EARLY_STOP_PATIENCE
    # Scores below this are counted as consecutive low results:
    early_stop_threshold = threshold * 0.4
    # A score above this triggers boosted patience:
    boost_trigger = threshold * 0.6
    for rank, (result, index_score) in enumerate(results):
        scoring = algorithm.compare(query=entity, result=result, config=config)
        log.debug(
            "Scoring result %s" % result.id,
            query_schema=entity.schema.name,
            result_id=result.id,
            result_schema=result.schema.name,
            algorithm=algorithm.__name__,
            rank=rank,
            algo_score=scoring.score,
            index_score=index_score,
        )
        # Yield control to the event loop
        # This might allow the event loop to process another request, resulting in
        # more even response times when CPU-bound scoring requests pile up.
        await asyncio.sleep(0)
        response = ScoredEntityResponse.from_entity_result(result, scoring, threshold)

        if response.score > early_stop_threshold:
            consecutive_low = 0
        else:
            consecutive_low += 1

        if response.score >= boost_trigger:
            patience = settings.SCORE_EARLY_STOP_PATIENCE * EARLY_STOP_BOOST_FACTOR

        if response.score <= cutoff:
            continue
        if response.match:
            matches += 1
        scored.append(response)

        if consecutive_low >= patience and rank >= limit:
            break

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return matches, scored
