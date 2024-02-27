import time
import asyncio
import warnings
from threading import Lock
from typing import cast, Any, Dict
from structlog.contextvars import get_contextvars
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.exceptions import TransportError, ConnectionError

from yente import settings
from yente.logs import get_logger

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = get_logger(__name__)
POOL: Dict[int, AsyncElasticsearch] = {}
query_semaphore = asyncio.Semaphore(settings.QUERY_CONCURRENCY)
index_lock = Lock()


def get_opaque_id() -> str:
    ctx = get_contextvars()
    return cast(str, ctx.get("trace_id"))


def get_es_connection() -> AsyncElasticsearch:
    """Get elasticsearch connection."""
    kwargs: Dict[str, Any] = dict(
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=10,
    )
    if settings.ES_SNIFF:
        kwargs["sniff_on_start"] = True
        kwargs["sniffer_timeout"] = 60
        kwargs["sniff_on_connection_fail"] = True
    if settings.ES_CLOUD_ID:
        log.info("Connecting to Elastic Cloud ID", cloud_id=settings.ES_CLOUD_ID)
        kwargs["cloud_id"] = settings.ES_CLOUD_ID
    else:
        kwargs["hosts"] = [settings.ES_URL]
    if settings.ES_USERNAME and settings.ES_PASSWORD:
        auth = (settings.ES_USERNAME, settings.ES_PASSWORD)
        kwargs["basic_auth"] = auth
    if settings.ES_CA_CERT:
        kwargs["ca_certs"] = settings.ES_CA_CERT
    return AsyncElasticsearch(**kwargs)


async def get_es() -> AsyncElasticsearch:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    if loop_id in POOL:
        return POOL[loop_id]

    for retry in range(2, 9):
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


async def close_es() -> None:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    es = POOL.pop(loop_id, None)
    if es is not None:
        log.info("Closing elasticsearch client")
        await es.close()
