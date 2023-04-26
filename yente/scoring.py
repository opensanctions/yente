from typing import Iterable, List, Optional, Dict, Callable
from followthemoney.types import registry
from jellyfish import soundex, jaro_winkler_similarity
from nomenklatura.entity import CE
from nomenklatura.matching import compare_scored
from nomenklatura.matching.types import MatchingResult
from nomenklatura.matching.features.util import compare_sets

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse
from yente.data.util import name_words


def _soundex_jaro(query: List[str], result: List[str]) -> float:
    """Compare two strings using the Soundex algorithm and Jaro-Winkler."""
    result_parts = name_words(result)
    result_soundex = [soundex(p) for p in result_parts]
    similiarities: List[float] = []
    for part in name_words(query):
        best = 0.0

        for other in result_parts:
            part_similarity = jaro_winkler_similarity(part, other)
            best = max(best, part_similarity)

        part_soundex = soundex(part)
        soundex_score = 1.0 if part_soundex in result_soundex else 0.0

        # OFAC is very unspecific on this part, so this is a best guess:
        part_score = (best + soundex_score) / 2

        similiarities.append(part_score)
    return sum(similiarities) / float(len(similiarities))


def _ofac_round_score(score: float, precision: float = 0.05):
    correction = 0.5 if score >= 0 else -0.5
    return round(int(score / precision + correction) * precision, 2)


def compare_ofac(query: CE, result: CE) -> MatchingResult:
    """Compare two entities in the manner described by OFAC."""
    # cf. https://ofac.treasury.gov/faqs/topic/1636
    query_names = query.get_type_values(registry.name, matchable=True)
    query_names = [n.lower() for n in query_names]
    result_names = result.get_type_values(registry.name, matchable=True)
    result_names = [n.lower() for n in result_names]

    names_jaro = compare_sets(query_names, result_names, jaro_winkler_similarity)
    soundex_jaro = _soundex_jaro(query_names, result_names)
    features = {"names_jaro": names_jaro, "soundex_jaro": soundex_jaro}
    score = _ofac_round_score(max(names_jaro, soundex_jaro))
    return MatchingResult(score=score, features=features)


SCORER: Dict[str, Callable[[CE, CE], MatchingResult]] = {
    "default": compare_scored,
    "ofac": compare_ofac,
}
DEFAULT_SCORER = "default"


def score_results(
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    scorer: str = DEFAULT_SCORER,
) -> List[ScoredEntityResponse]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    scorer_func = SCORER.get(scorer, SCORER[DEFAULT_SCORER])
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
