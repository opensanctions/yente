import httpx
import warnings
from pathlib import Path
from functools import cache
from urllib.parse import urlparse
from prefixdate.precision import Precision
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List, Optional, Set, Generator
from rigour.names import Symbol
from followthemoney import EntityProxy, Property, Schema, registry
from followthemoney.util import sanitize_text

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


# A set of symbol categories that we don't want to match on and therefore don't want to index.
NON_MATCHABLE_SYMBOLS = {Symbol.Category.INITIAL}


def extract_values(values: Any) -> List[str]:
    """Extract a list of string values from a property value, which may be a string or a list."""
    sanitized: List[str | None] = []
    if isinstance(values, (list, tuple, set)):
        sanitized = [sanitize_text(v) for v in values]
    else:
        sanitized = [sanitize_text(values)]
    return [v for v in sanitized if v is not None and len(v) > 0]


def index_symbols(symbols: Set[Symbol]) -> Generator[str, None, None]:
    """Get the set of symbols to be indexed for a given name."""
    for symbol in symbols:
        if symbol.category not in NON_MATCHABLE_SYMBOLS:
            yield f"{symbol.category.value}:{symbol.id}"


@cache
def _entity_weak_props(schema: Schema) -> Set[Property]:
    """Get the set of properties that are not used for matching but can be used for
    display."""
    weak_props: Set[Property] = set()
    for prop in schema.properties.values():
        if prop.type == registry.name and not prop.matchable:
            weak_props.add(prop)
    return weak_props


def entity_weak_names(entity: EntityProxy) -> Set[str]:
    """Get a set of weak names for an entity, which are the names that are not used for
    matching but can be used for display."""
    weak_names: Set[str] = set()
    for prop in _entity_weak_props(entity.schema):
        for value in entity.get_prop(prop):
            weak_names.add(value.casefold())
    return weak_names


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
