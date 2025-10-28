import asyncio
from typing import Iterable, List, Optional, Type, Tuple
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse


async def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    config: ScoringConfig = ScoringConfig.defaults(),
) -> Tuple[int, List[ScoredEntityResponse]]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    for result in results:
        scoring = algorithm.compare(query=entity, result=result, config=config)
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
