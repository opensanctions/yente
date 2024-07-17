from typing import AsyncIterator
from contextlib import asynccontextmanager
import asyncio

from yente import settings
from yente.logs import get_logger
from yente.provider.elastic import ElasticSearchProvider
from yente.provider.opensearch import OpenSearchProvider
from yente.provider.base import SearchProvider

log = get_logger(__name__)

__all__ = ["with_provider", "SearchProvider"]

PROVIDERS: dict[int, SearchProvider] = {}


async def get_provider() -> SearchProvider:
    if settings.INDEX_TYPE == "opensearch":
        return await OpenSearchProvider.create()
    else:
        return await ElasticSearchProvider.create()


@asynccontextmanager
async def with_provider() -> AsyncIterator[SearchProvider]:
    try:
        loop_id = id(asyncio.get_event_loop())
        if loop_id in PROVIDERS:
            provider = PROVIDERS[loop_id]
        else:
            provider = await get_provider()
            PROVIDERS[loop_id] = provider
        yield provider
    finally:
        PROVIDERS.pop(id(asyncio.get_event_loop()), None)
        await provider.close()
