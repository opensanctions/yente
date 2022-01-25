import time
import logging
import warnings
from asyncstdlib.functools import cache
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.exceptions import TransportError, ConnectionError

from yente.settings import ES_URL

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger(__name__)


@cache
async def get_es():
    log.info("Connection to ES at: %s", ES_URL)
    es = AsyncElasticsearch(hosts=[ES_URL])
    for retry in range(7):
        try:
            await es.cluster.health(wait_for_status="yellow", request_timeout=5)
            return es
        except (TransportError, ConnectionError) as exc:
            log.exception("Cannot connect to ElasticSearch")
            time.sleep(retry ** 2)
    raise RuntimeError("Cannot connect to ElasticSearch")
