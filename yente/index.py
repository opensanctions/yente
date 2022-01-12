import warnings

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from osapi.settings import ES_URL

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

es = AsyncElasticsearch(hosts=[ES_URL])
