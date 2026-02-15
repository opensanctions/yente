import asyncio
from typing import Iterable, List, Optional, Type, Tuple
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse

log = get_logger(__name__)


async def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Tuple[Entity, float]],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    config: ScoringConfig = ScoringConfig.defaults(),
) -> Tuple[int, List[ScoredEntityResponse]]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    for rank, (result, index_score) in enumerate(results):
        scoring = algorithm.compare(query=entity, result=result, config=config)
        log.info(
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
        if response.score <= cutoff:
            continue
        if response.match:
            matches += 1
        scored.append(response)

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return matches, scored
