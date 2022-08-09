from functools import lru_cache
import fingerprints
from normality import WS
from datetime import datetime
from Levenshtein import distance  # type: ignore
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator, Dict, List, Set, cast
from followthemoney.types import registry


def iso_datetime(value: str) -> datetime:
    """Parse a second-precision ISO date time string."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def iso_to_version(value: str) -> str:
    dt = iso_datetime(value)
    return dt.strftime("%Y%m%d%H%M%S")


def expand_dates(dates: List[str]) -> List[str]:
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)


def expand_names(names: List[str]) -> List[str]:
    """Expand names into normalized version."""
    expanded = set(names)
    for name in names:
        fp = fingerprints.generate(name)
        if fp is not None:
            expanded.add(fp)
    return list(expanded)


def tokenize_names(names: List[str]) -> Set[str]:
    """Get a unique set of tokens present in the given set of names."""
    expanded = set()
    for name in names:
        name = name.lower()
        expanded.update(name.split(WS))
        fp = fingerprints.generate(name)
        if fp is not None:
            expanded.update(fp.split(WS))
    return expanded


@lru_cache(maxsize=500)
def _compare_distance(left: str, right: str) -> int:
    dist: int = distance(left.lower()[:250], right.lower()[:250])
    return dist

def pick_names(names: List[str], limit: int=3) -> List[str]:
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
        
        pick, _ = sorted(candidates.items(), key=lambda c: c[1], reverse=True)[0]
        picked.append(pick)

    return picked



@asynccontextmanager
async def http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(
        total=3600,
        connect=None,
        sock_connect=None,
        sock_read=None,
    )
    async with ClientSession(timeout=timeout, trust_env=True) as client:
        yield client
