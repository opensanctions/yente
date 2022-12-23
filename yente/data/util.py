import fingerprints
from pathlib import Path
from functools import lru_cache
from urllib.parse import urlparse
from normality import WS
from Levenshtein import distance
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator, Dict, List, Optional, Set, Union
from followthemoney.types import registry


def expand_dates(dates: List[str]) -> List[str]:
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)


@lru_cache(maxsize=500)
def fingerprint_name(name: str) -> Optional[str]:
    return fingerprints.generate(name)


def expand_names(names: List[str]) -> List[str]:
    """Expand names into normalized version."""
    expanded = set(names)
    for name in names:
        fp = fingerprint_name(name)
        if fp is not None:
            expanded.add(fp)
    return list(expanded)


def tokenize_names(names: List[str]) -> Set[str]:
    """Get a unique set of tokens present in the given set of names."""
    expanded = set()
    for name in names:
        name = name.lower()
        expanded.update(name.split(WS))
        fp = fingerprint_name(name)
        if fp is not None:
            expanded.update(fp.split(WS))
    return expanded


@lru_cache(maxsize=500)
def _compare_distance(left: str, right: str) -> int:
    dist: int = distance(left[:250], right[:250])
    return dist


def pick_names(names: List[str], limit: int = 3) -> List[str]:
    """Try to pick a few non-overlapping names to search for when matching
    an entity. The problem here is that if we receive an API query for an
    entity with hundreds of aliases, it becomes prohibitively expensive to
    search. This function decides which ones should be queried as pars pro
    toto in the index before the Python comparison algo later checks all of
    them.

    This is a bit over the top and will come back to haunt me."""
    if len(names) <= limit:
        return names
    picked: List[str] = []
    fingerprinted_ = [fingerprint_name(n) for n in names]
    names = [n for n in fingerprinted_ if n is not None]

    # Centroid:
    picked_name = registry.name.pick(names)
    if picked_name is not None:
        picked.append(picked_name)

    # Pick the least similar:
    for _ in range(1, limit):
        candidates: Dict[str, int] = {}
        for cand in names:
            if cand in picked:
                continue
            candidates[cand] = 0
            for pick in picked:
                candidates[cand] += _compare_distance(pick, cand)

        if not len(candidates):
            break
        pick, _ = sorted(candidates.items(), key=lambda c: c[1], reverse=True)[0]
        picked.append(pick)

    return picked


def resolve_url_type(url: str) -> Union[Path, str]:
    """Check if a given path is local or remote and return a parsed form."""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme in ("http", "https"):
        return url
    if parsed.path:
        path = Path(parsed.path).resolve()
        if path.exists():
            return path
    raise RuntimeError("Cannot open resource: %s" % url)


@asynccontextmanager
async def http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(
        total=84600,
        connect=None,
        sock_connect=None,
        sock_read=None,
    )
    async with ClientSession(timeout=timeout, trust_env=True) as client:
        yield client
