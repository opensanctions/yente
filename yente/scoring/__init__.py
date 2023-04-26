from typing import Iterable, List, Optional, Dict, Callable
from nomenklatura.entity import CE
from nomenklatura.matching import compare_scored
from nomenklatura.matching.types import MatchingResult

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse
from yente.scoring.ofac import compare_ofac


ALGORITHMS: Dict[str, Callable[[CE, CE], MatchingResult]] = {
    "regression_matcher": compare_scored,
    "ofac_249": compare_ofac,
}
DEFAULT_ALGORITHM = "regression_matcher"


def score_results(
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    algorithm: str = DEFAULT_ALGORITHM,
) -> List[ScoredEntityResponse]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    scorer_func = ALGORITHMS.get(algorithm, ALGORITHMS[DEFAULT_ALGORITHM])
    for proxy in results:
        scoring = scorer_func(entity, proxy)
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
