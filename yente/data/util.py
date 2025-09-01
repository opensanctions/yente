import itertools
import warnings
from followthemoney import EntityProxy
import httpx
import unicodedata
from pathlib import Path
from functools import lru_cache
from urllib.parse import urlparse
from followthemoney.types import registry
from followthemoney.schema import Schema
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from normality import squash_spaces, ascii_text
from typing import AsyncGenerator, Dict, List, Optional, Set, Generator
from rigour.text.scripts import is_modern_alphabet
from rigour.text import levenshtein, metaphone
from rigour.names import tokenize_name, remove_person_prefixes
from rigour.names import replace_org_types_compare
from rigour.names.tokenize import normalize_name
from rigour.names import tag_person_name, Name, tag_org_name, Symbol

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


# A set of symbol categories that we don't want to match on and therefore don't want to index.
NON_MATCHABLE_SYMBOLS = {Symbol.Category.INITIAL}


def preprocess_name(name: Optional[str]) -> Optional[str]:
    """Preprocess a name for comparison."""
    if name is None:
        return None
    name = unicodedata.normalize("NFC", name)
    name = name.lower()
    return squash_spaces(name)


@lru_cache(maxsize=2000)
def clean_tokenize_name(schema: Schema, name: str) -> List[str]:
    """Tokenize a name and clean it up."""
    name = preprocess_name(name) or name
    if schema.name in ("LegalEntity", "Organization", "Company", "PublicBody"):
        name = replace_org_types_compare(name, normalizer=preprocess_name)
    elif schema.name in ("LegalEntity", "Person"):
        name = remove_person_prefixes(name)
    return tokenize_name(name)


def phonetic_names(schema: Schema, names: List[str]) -> Set[str]:
    """Generate phonetic forms of the given names."""
    phonemes: Set[str] = set()
    for name in names:
        for token in clean_tokenize_name(schema, name):
            if len(token) < 3 or not is_modern_alphabet(token):
                continue
            if token.isnumeric():
                continue
            phoneme = metaphone(ascii_text(token))
            if len(phoneme) > 2:
                phonemes.add(phoneme)
    return phonemes


def index_name_parts(schema: Schema, names: List[str]) -> Set[str]:
    """Generate a list of indexable name parts from the given names."""
    parts: Set[str] = set()
    for name in names:
        for token in clean_tokenize_name(schema, name):
            if len(token) < 2:
                continue
            parts.add(token)
            # TODO: put name and company symbol lookups here
            if is_modern_alphabet(token):
                ascii_token = ascii_text(token)
                if ascii_token is not None and len(ascii_token) > 1:
                    parts.add(ascii_token)
    return parts


def index_name_keys(schema: Schema, names: List[str]) -> Set[str]:
    """Generate a indexable name keys from the given names."""
    keys: Set[str] = set()
    for name in names:
        tokens = clean_tokenize_name(schema, name)
        ascii_tokens: List[str] = []
        for token in tokens:
            if token.isnumeric() or not is_modern_alphabet(token):
                ascii_tokens.append(token)
                continue
            ascii_token = ascii_text(token) or token
            ascii_tokens.append(ascii_token)
        ascii_name = "".join(sorted(ascii_tokens))
        if len(ascii_name) > 5:
            keys.add(ascii_name)
    return keys


def build_index_name_symbols(entity: EntityProxy) -> List[str]:
    """Build a list of indexable name symbols for the given entity.

    If we can't build symbols for this schema, return an empty list."""
    names: List[str] = entity.get_type_values(registry.name, matchable=True)

    symbols: Set[Symbol] = set()
    # normalizer should be the same as that one used in logic-v2 to make the @lru_cache do most of the work
    # For both Person and Organization, build all symbols for all names
    if entity.schema.is_a("Person"):
        symbols = set(
            itertools.chain.from_iterable(
                tag_person_name(Name(name), normalize_name).symbols for name in names
            )
        )
    elif entity.schema.is_a("Organization"):
        symbols = set(
            itertools.chain.from_iterable(
                tag_org_name(Name(name), normalize_name).symbols for name in names
            )
        )
    else:
        # We can't build name symbols for this schema
        symbols = set()

    # Exclude symbols that we don't want to match on
    symbols = {s for s in symbols if s.category not in NON_MATCHABLE_SYMBOLS}

    # Note that this value is used as a keyword in the search index, so changing the format requires
    # upping the index version to trigger a re-index.
    return [f"{s.category.value}:{s.id}" for s in symbols]


def pick_names(names: List[str], limit: int = 3) -> List[str]:
    """Try to pick a few non-overlapping names to search for when matching
    an entity. The problem here is that if we receive an API query for an
    entity with hundreds of aliases, it becomes prohibitively expensive to
    search. This function decides which ones should be queried as pars pro
    toto in the index before the Python comparison algo later checks all of
    them.

    This is a bit over the top and will come back to haunt us."""
    if len(names) <= limit:
        return names
    picked: List[str] = []
    processed_ = [preprocess_name(n) for n in names]
    names = [n for n in processed_ if n is not None]

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


def expand_dates(dates: List[str]) -> List[str]:
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)


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


class Authenticator(httpx.Auth):
    def __init__(self, auth_token: Optional[str] = None):
        self.auth_token = auth_token

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        if self.auth_token:
            request.headers["Authorization"] = f"Token {self.auth_token}"

        if settings.DATA_TOKEN is not None:
            warnings.warn(
                "settings.DATA_TOKEN (which sets the Authentication header) is deprecated "
                "and will be removed in a future release of Yente. Use auth_token in "
                "manifest instead (which sets the Authorization header).",
                DeprecationWarning,
            )
            request.headers["Authentication"] = f"Token {settings.DATA_TOKEN}"
        yield request


@asynccontextmanager
async def httpx_session(
    auth_token: Optional[str] = None,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    transport = httpx.AsyncHTTPTransport(retries=3)
    proxy = settings.HTTP_PROXY if settings.HTTP_PROXY != "" else None
    headers = {"User-Agent": f"Yente/{settings.VERSION}"}
    async with httpx.AsyncClient(
        transport=transport,
        http2=True,
        timeout=None,
        proxy=proxy,
        headers=headers,
        auth=Authenticator(auth_token),
        follow_redirects=True,
    ) as client:
        yield client
