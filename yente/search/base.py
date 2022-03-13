import time
import asyncio
import logging
from typing import Dict
import warnings
from asyncstdlib.functools import cache
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.exceptions import TransportError, ConnectionError

from yente import settings

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger(__name__)
POOL: Dict[int, AsyncElasticsearch] = {}


# @cache
async def get_es() -> AsyncElasticsearch:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    if loop_id in POOL:
        return POOL[loop_id]

    log.info("Connection to ES at: %s", settings.ES_URL)
    es = AsyncElasticsearch(
        hosts=[settings.ES_URL],
        # max_retries=10,
        # retry_on_timeout=True,
        # sniff_on_connection_fail=True,
    )
    for retry in range(7):
        try:
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
