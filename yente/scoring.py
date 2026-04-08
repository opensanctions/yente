import asyncio
from typing import Iterable, List, Type, Tuple
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse

log = get_logger(__name__)

# Early stopping via score budget: candidates from ES are scored one by one by the
# matching algorithm (e.g. LogicV2). In production, ~82% of scoring calls produce scores
# below cutoff, and ~49% of queries have zero candidates above 0.5. To avoid wasting CPU,
# we maintain a budget that drains with each low-scoring candidate and refills with each
# good one:
#
#   budget = budget - 1 + score / (threshold / 2)
#
# A score of threshold/2 breaks even. Higher scores extend the search; lower scores drain
# the budget. When the budget is exhausted, we stop. This naturally adapts to query
# quality: queries with real matches keep searching proportionally longer.
#
# Caveat: this can miss results buried deep in the ES ranking. In production log analysis
# (418 queries), budget=10 missed 3 results (0.7%), all sub-threshold (highest 0.592 vs
# 0.7 threshold). Set YENTE_SCORE_EARLY_STOP_BUDGET high to disable.
EARLY_STOP_BREAK_EVEN = 0.5  # fraction of threshold where budget breaks even


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
    tau = threshold * EARLY_STOP_BREAK_EVEN
    budget = float(settings.SCORE_EARLY_STOP_BUDGET) if tau > 0 else float("inf")
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

        budget = budget - 1.0 + response.score / tau

        if response.score > cutoff:
            if response.match:
                matches += 1
            scored.append(response)

        if budget <= 0 and rank >= limit:
            break

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return matches, scored
