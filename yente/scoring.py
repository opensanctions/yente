from typing import Any, Dict, Iterable, List
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.matching import compare_scored

from yente import settings
from yente.entity import Entity


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
) -> List[Dict[str, Any]]:
    scored: List[Dict[str, Any]] = []
    matches = 0
    for res in results:
        result = res.to_dict()
        result.update(compare_scored(entity, res))
        result["match"] = result["score"] >= threshold
        if result["match"]:
            matches += 1
        scored.append(result)

    scored = sorted(scored, key=lambda r: r["score"], reverse=True)

    # If multiple entities meet the match threshold, it's ambiguous
    # and we bail out:
    if matches > 1:
        for result in scored:
            result["match"] = False

    return scored
