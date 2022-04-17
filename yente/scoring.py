from typing import Any, Dict, Iterable, List
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.matching import compare_scored

from yente import settings
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse


def prepare_entity(data: Dict[str, Any]) -> Entity:
    """Generate an entity from user data for matching."""
    data["id"] = "query"
    proxy = Entity.from_os_data(data, {}, cleaned=False)

    # Generate names from name parts
    combine_names(proxy)

    # Extract names from IBANs, phone numbers etc.
    countries = proxy.get_type_values(registry.country)
    for (prop, value) in proxy.itervalues():
        hint = prop.type.country_hint(value)
        if hint is not None and hint not in countries:
            proxy.add("country", hint, cleaned=True)
    return proxy


def score_results(
    entity: Entity,
    results: Iterable[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
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

    # If multiple entities meet the match threshold, it's ambiguous
    # and we bail out:
    if matches > 1:
        for result in scored:
            result.match = False

    return scored
