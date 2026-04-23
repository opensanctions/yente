import asyncio
from typing import AsyncIterator
from contextlib import asynccontextmanager

from yente import settings
from yente.logs import get_logger
from yente.provider.elastic import ElasticSearchProvider
from yente.provider.opensearch import OpenSearchProvider
from yente.provider.base import SearchProvider

log = get_logger(__name__)

__all__ = ["with_provider", "get_provider", "close_provider", "SearchProvider"]

PROVIDERS: dict[int, SearchProvider] = {}


def get_id() -> int:
    return id(asyncio.get_running_loop())


async def _create_provider() -> SearchProvider:
    """Create the search provider based on the configured index type."""
    if settings.INDEX_TYPE == "opensearch":
        return await OpenSearchProvider.create()
    else:
        return await ElasticSearchProvider.create()


async def get_provider() -> SearchProvider:
    """Get the search provider for the current event loop, or create it."""
    loop_id = get_id()
    if loop_id not in PROVIDERS:
        PROVIDERS[loop_id] = await _create_provider()
    return PROVIDERS[loop_id]


async def close_provider() -> None:
    """Close all cached providers. Short-lived event loops (e.g. Starlette's
    TestClient portal) leave stranded entries keyed to dead loops; iterate
    so they don't pile up and leak the aiohttp connectors they hold."""
    providers = list(PROVIDERS.values())
    PROVIDERS.clear()
    for provider in providers:
        try:
            await provider.close()
        except Exception as exc:
            log.warning("Failed to close search provider: %r", exc)


@asynccontextmanager
async def with_provider() -> AsyncIterator[SearchProvider]:
    provider = await _create_provider()
    try:
        yield provider
    finally:
        await provider.close()
