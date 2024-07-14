from typing import AsyncIterator
from contextlib import asynccontextmanager

from yente import settings
from yente.logs import get_logger
from yente.provider.elastic import ElasticSearchProvider
from yente.provider.opensearch import OpenSearchProvider
from yente.provider.base import SearchProvider

log = get_logger(__name__)

__all__ = ["with_provider", "SearchProvider"]


@asynccontextmanager
async def with_provider() -> AsyncIterator[SearchProvider]:
    if settings.INDEX_TYPE == "opensearch":
        provider: SearchProvider = await OpenSearchProvider.create()
    else:
        provider = await ElasticSearchProvider.create()
    try:
        yield provider
    finally:
        await provider.close()
