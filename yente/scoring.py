from typing import Any, Dict, List
from followthemoney import model
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.entity import CompositeEntity as Entity
from nomenklatura.matching import compare_scored

from yente import settings


def prepare_entity(data: Dict[str, Any]) -> Entity:
    """Generate an entity from user data for matching."""
    proxy = Entity.from_data(model, data, {}, cleaned=False)

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
    results: List[Entity],
    threshold: float = settings.SCORE_THRESHOLD,
) -> List[Dict[str, Any]]:
    scored: List[Dict[str, Any]] = []
    for res in results:
        result = res.to_dict()
        result.update(compare_scored(entity, res))
        result["match"] = False
        scored.append(result)

    scored = sorted(scored, key=lambda r: r["score"], reverse=True)

    # Set match if the first result meets threshold, and no others:
    if len(scored) > 0 and scored[0]["score"] >= threshold:
        scored[0]["match"] = True
        if len(scored) > 1 and scored[1]["score"] >= threshold:
            scored[0]["match"] = False

    return scored
