from typing import Iterable, List, Optional, Type, Dict, Tuple
from nomenklatura.matching.types import ScoringAlgorithm

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse


def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    weights: Dict[str, float] = {},
) -> Tuple[int, List[ScoredEntityResponse]]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    for proxy in results:
        scoring = algorithm.compare(entity, proxy, override_weights=weights)
        result = ScoredEntityResponse.from_entity_result(proxy, scoring, threshold)
        if result.score <= cutoff:
            continue
        if result.match:
            matches += 1
        scored.append(result)

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return matches, scored
