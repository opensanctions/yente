from typing import Any, Optional, Tuple
from pydantic import AnyHttpUrl
from pydantic.type_adapter import TypeAdapter

from yente import settings


class EntityRedirect(Exception):
    def __init__(self, canonical_id: str) -> None:
        self.canonical_id = canonical_id


def typed_url(url: Any) -> AnyHttpUrl:
    return TypeAdapter(AnyHttpUrl).validate_python(url)


def match_prefix(prefix: str, *labels: Optional[str]) -> bool:
    prefix = prefix.lower().strip()
    if not len(prefix):
        return False
    for label in labels:
        if label is None:
            continue
        label = label.lower().strip()
        if label.startswith(prefix):
            return True
    return False


def limit_window(limit: Any, offset: Any, default_limit: int = 10) -> Tuple[int, int]:
    """ElasticSearch can only return results from within a window of the first 10,000
    scored results. This means that offset + limit may never exceed 10,000 - so here's
    a bunch of bounding magic to enforce that."""
    try:
        num_limit = max(0, int(limit))
    except (ValueError, TypeError):
        num_limit = default_limit
    try:
        num_offset = max(0, int(offset))
        num_offset = min(settings.MAX_RESULTS, num_offset)
    except (ValueError, TypeError):
        num_offset = 0
    end = num_limit + num_offset
    if end > settings.MAX_RESULTS:
        num_limit = max(0, settings.MAX_RESULTS - num_offset)
    return num_limit, num_offset
