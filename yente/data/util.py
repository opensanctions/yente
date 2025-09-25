import warnings
from followthemoney import EntityProxy
import httpx
import unicodedata
from httpx_retries import Retry, RetryTransport
from pathlib import Path
from urllib.parse import urlparse
from followthemoney.types import registry
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from normality import squash_spaces
from normality.cleaning import remove_unsafe_chars
from typing import AsyncGenerator, Dict, List, Optional, Set, Generator
from rigour.text import levenshtein
from rigour.names import remove_person_prefixes
from rigour.names import replace_org_types_compare
from rigour.names.tokenize import normalize_name, prenormalize_name
from rigour.names import tag_person_name, Name, tag_org_name, Symbol, NameTypeTag
from followthemoney.names import schema_type_tag

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


# A set of symbol categories that we don't want to match on and therefore don't want to index.
NON_MATCHABLE_SYMBOLS = {Symbol.Category.INITIAL}


def preprocess_name(name: Optional[str]) -> Optional[str]:
    """Preprocess a name for comparison."""
    if name is None:
        return None
    name = name.lower()
    return squash_spaces(name)


def safe_string(value: str) -> str:
    """Make sure a value coming from the API is a safe string for data comparison."""
    value = unicodedata.normalize("NFC", value)
    value = remove_unsafe_chars(value)
    return value.strip()


def entity_names(entity: EntityProxy) -> Set[Name]:
    """Build name objects from the names linked to an entity."""
    # TODO: this does ca. the same thing as `logic_v2.names.analysis`. Should we extract that into
    # followthemoney or has it not yet stabilised enough?
    name_type = schema_type_tag(entity.schema)
    names: Set[Name] = set()

    is_org = name_type in (NameTypeTag.ORG, NameTypeTag.ENT)
    is_person = name_type == NameTypeTag.PER

    values = entity.get_type_values(registry.name, matchable=True)
    values.extend(entity.get("weakAlias", quiet=True))
    for value in values:
        if name_type == NameTypeTag.PER:
            value = remove_person_prefixes(value)
        norm = prenormalize_name(value)
        if is_org:
            norm = replace_org_types_compare(norm, normalizer=prenormalize_name)
        name = Name(value, form=norm, tag=name_type)

        # Apply symbols:
        if is_person:
            tag_person_name(name, normalize_name)
        if is_org:
            tag_org_name(name, normalize_name)
        names.add(name)
    return names


def is_matchable_symbol(symbol: Symbol) -> bool:
    """Check if a symbol is matchable."""
    return symbol.category not in NON_MATCHABLE_SYMBOLS


def index_symbol(symbol: Symbol) -> str:
    return f"{symbol.category.value}:{symbol.id}"


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
    retry = Retry(total=3, backoff_factor=2)
    transport = RetryTransport(transport=httpx.AsyncHTTPTransport(), retry=retry)
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
