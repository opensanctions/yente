from pathlib import Path
from jellyfish import metaphone
from urllib.parse import urlparse
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from typing import AsyncGenerator, Dict, List, Set, Union
from followthemoney.types import registry
from nomenklatura.util import fingerprint_name, name_words, levenshtein


def expand_dates(dates: List[str]) -> List[str]:
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)


def phonetic_names(names: List[str]) -> List[str]:
    """Generate phonetic forms of the given names."""
    phonemes: Set[str] = set()
    for word in name_words(names):
        if len(word) > 2:
            phonemes.add(metaphone(word))
    return list(phonemes)


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
                candidates[cand] += levenshtein(pick, cand)

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
        connect=30,
        sock_connect=None,
        sock_read=None,
    )
    connector = TCPConnector(limit=10)
    async with ClientSession(
        timeout=timeout,
        trust_env=True,
        connector=connector,
        read_bufsize=10 * 1024 * 1024,
    ) as client:
        yield client
