from typing import Iterable, List, Optional
from nomenklatura.matching import compare_scored

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse


def score_results(
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
) -> List[ScoredEntityResponse]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    for proxy in results:
        scoring = compare_scored(entity, proxy)
        result = ScoredEntityResponse.from_entity_result(proxy, scoring, threshold)
        if result.score <= cutoff:
            continue
        if result.match:
            matches += 1
        scored.append(result)

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]

    # If multiple entities meet the match threshold, it's ambiguous
    # and we bail out:
    if matches > 1:
        for result in scored:
            result.match = False

    return scored
