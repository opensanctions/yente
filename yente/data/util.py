from pathlib import Path
from normality import WS
from urllib.parse import urlparse
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from typing import AsyncGenerator, Dict, List, Union, Iterable, Optional
from followthemoney.types import registry
from fingerprints import remove_types, clean_name_light
from nomenklatura.util import fingerprint_name, levenshtein, phonetic_token
from nomenklatura.util import names_word_list


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
    phonemes: List[str] = []
    for word in names_word_list(names, min_length=2):
        phonemes.append(phonetic_token(word))
    return phonemes


def _name_parts(name: Optional[str]) -> Iterable[str]:
    if name is None:
        return
    for part in name.split(WS):
        if len(part) > 1:
            yield part


def index_name_parts(names: List[str]) -> List[str]:
    """Generate a list of indexable name parts from the given names."""
    parts: List[str] = []
    for name in names:
        fp = fingerprint_name(name)
        parts.extend(_name_parts(fp))
        cleaned = remove_types(name, clean=clean_name_light)
        parts.extend(_name_parts(cleaned))
    return parts


def index_name_keys(names: List[str]) -> List[str]:
    """Generate a indexable name keys from the given names."""
    keys: List[str] = []
    for name in names:
        for key in (fingerprint_name(name), clean_name_light(name)):
            if key is not None:
                keys.append(key)
    return keys


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
