import httpx
import os
from pathlib import Path
from normality import WS
from urllib.parse import urlparse
from jellyfish import metaphone
from functools import lru_cache
from followthemoney.types import registry
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Iterable, Optional, Set
from rigour.text.scripts import is_modern_alphabet
from rigour.text.distance import levenshtein
from fingerprints import remove_types, clean_name_light
from nomenklatura.util import fingerprint_name, names_word_list


@lru_cache(maxsize=5000)
def _metaphone_cached(word: str) -> str:
    return metaphone(word)


def _clean_phonetic(original: str) -> Optional[str]:
    # We're being extra picky what phonemes are put into the search index,
    # so that we can reduce the number of false positives.
    if not is_modern_alphabet(original):
        return None
    return fingerprint_name(original)


def _proxy_env():
    """Retrieve proxy from env vars if set."""
    return os.environ.get(
        "HTTP_PROXY", os.environ.get("HTTPS_PROXY", os.environ.get("ALL_PROXY"))
    )


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
    for word in names_word_list(names, normalizer=_clean_phonetic, min_length=2):
        token = _metaphone_cached(word)
        if len(token) > 2:
            phonemes.append(token)
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
    keys: Set[str] = set()
    for name in names:
        for key in (fingerprint_name(name), clean_name_light(name)):
            if key is not None:
                key = key.replace(" ", "")
                keys.add(key)
    return list(keys)


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


def get_url_local_path(url: str) -> Optional[Path]:
    """Check if a given URL is local file path."""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme in ("file", "") and parsed.path != "":
        path = Path(parsed.path).resolve()
        if not path.exists():
            raise RuntimeError("File not found: %s" % path)
        return path
    return None


@asynccontextmanager
async def httpx_session() -> AsyncGenerator[httpx.AsyncClient, None]:
    transport = httpx.AsyncHTTPTransport(retries=3)
    async with httpx.AsyncClient(
        transport=transport, http2=True, timeout=None, proxy=_proxy_env
    ) as client:
        yield client
