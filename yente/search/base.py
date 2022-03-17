import time
import asyncio
import logging
from typing import Any, Dict
import warnings
from asyncstdlib.functools import cache
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.exceptions import TransportError, ConnectionError

from yente import settings

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger(__name__)
POOL: Dict[int, AsyncElasticsearch] = {}


def get_es_connection() -> AsyncElasticsearch:
    """Get elasticsearch connection."""
    kwargs: Dict[str, Any] = dict(
        timeout=120,
        request_timeout=120,
        retry_on_timeout=True,
        max_retries=5,
    )
    if settings.ES_CLOUD_ID:
        log.info("Connecting to Elastic Cloud ID: %s", settings.ES_CLOUD_ID)
        kwargs["cloud_id"] = settings.ES_CLOUD_ID
    else:
        kwargs["hosts"] = [settings.ES_URL]
    if settings.ES_USERNAME and settings.ES_PASSWORD:
        auth = (settings.ES_USERNAME, settings.ES_PASSWORD)
        kwargs["basic_auth"] = auth
    return AsyncElasticsearch(**kwargs)


# @cache
async def get_es() -> AsyncElasticsearch:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    if loop_id in POOL:
        return POOL[loop_id]

    for retry in range(7):
        try:
            es = get_es_connection()
            es_ = es.options(request_timeout=5)
            await es_.cluster.health(wait_for_status="yellow")
            POOL[loop_id] = es
            return POOL[loop_id]
        except (TransportError, ConnectionError) as exc:
            log.error("Cannot connect to ElasticSearch: %r" % exc)
            time.sleep(retry**2)
    raise RuntimeError("Cannot connect to ElasticSearch")


async def close_es():
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    es = POOL.pop(loop_id, None)
    if es is not None:
        log.info("Closing elasticsearch client")
        await es.close()
