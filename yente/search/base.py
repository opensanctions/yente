import time
import logging
import warnings
from asyncstdlib.functools import cache
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.exceptions import TransportError, ConnectionError

from yente import settings

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger(__name__)


@cache
async def get_es() -> AsyncElasticsearch:
    log.info("Connection to ES at: %s", settings.ES_URL)
    es = AsyncElasticsearch(hosts=[settings.ES_URL])
    for retry in range(7):
        try:
            es_ = es.options(request_timeout=5)
            await es_.cluster.health(wait_for_status="yellow")
            return es
        except (TransportError, ConnectionError) as exc:
            log.exception("Cannot connect to ElasticSearch")
            time.sleep(retry ** 2)
    raise RuntimeError("Cannot connect to ElasticSearch")
