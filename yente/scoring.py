from fastapi import HTTPException
from typing import Iterable, List, Optional, Type
from nomenklatura.matching import ALGORITHMS, MatcherV1
from nomenklatura.matching.types import ScoringAlgorithm

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse


def get_algorithm(name: str) -> Type[ScoringAlgorithm]:
    """Return the scoring algorithm class with the given name."""
    for algorithm in ALGORITHMS:
        if algorithm.NAME == name:
            return algorithm
    raise HTTPException(400, detail=f"Unknown algorithm: {name}")


def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
) -> List[ScoredEntityResponse]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    for proxy in results:
        scoring = algorithm.compare(entity, proxy)
        result = ScoredEntityResponse.from_entity_result(proxy, scoring, threshold)
        if result.score <= cutoff:
            continue
        if result.match:
            matches += 1
        scored.append(result)

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return scored
