import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from banal import as_bool
from os import environ as env
from normality import stringify
from datetime import datetime
from aiocron import Cron  # type: ignore
import random


def env_get(name: str) -> Optional[str]:
    """Ensure the env returns a string even on Windows (#100)."""
    return stringify(env.get(name))


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


def random_cron() -> str:
    """Randomize the minute of the cron schedule to avoid thundering herd problem."""
    random_minute = str(random.randint(0, 59))
    return f"{random_minute} */2 * * *"


VERSION = "3.8.10"
AUTHOR = "OpenSanctions"
HOME_PAGE = "https://www.opensanctions.org"
EMAIL = "info@opensanctions.org"
CONTACT = {"name": AUTHOR, "url": HOME_PAGE, "email": EMAIL}

TITLE = env_str("YENTE_TITLE", "yente")
DESCRIPTION = """
The yente API provides endpoints that help you determine if any of the people or
companies mentioned in your data are subject to international sanctions, known
to be involved in criminal activity, or if they are politically exposed people.

`yente` is the open source basis for the OpenSanctions SaaS API. Its matching
and entity retrieval functionality is identical to the hosted API, but it does
not include functionality for metered accounting of API requests.

In this service, there is support for the following operations:

* A simple text-based search for interactive applications (``/search``),
* A query-by-example endpoint for screening tasks (``/match``),
* Support for getting graph data for a particular entity (``/entities``),
* Support for the OpenRefine Reconciliation API (``/reconcile``).

The API uses JSON for data transfer and does not support authentication or access
control.

Further reading:

* [Self-hosted OpenSanctions](https://www.opensanctions.org/docs/self-hosted/)
* [Install and deployment](https://www.opensanctions.org/docs/yente/)
* Intro to the [entity data model](https://www.opensanctions.org/docs/entities/)
* Tutorial: [Using the matching API to do screening checks](https://www.opensanctions.org/docs/api/matching/)
* [Data dictionary](https://opensanctions.org/reference/)
"""

TAGS: List[Dict[str, Any]] = [
    {
        "name": "Matching",
        "description": "Endpoints for conducting a user-facing entity search or "
        "matching a local data store against the given dataset.",
        "externalDocs": {
            "description": "Data dictionary",
            "url": "https://opensanctions.org/reference/",
        },
    },
    {
        "name": "System information",
        "description": "Service metadata endpoints for health checking and getting "
        "the application metadata to be used in client applications.",
    },
    {
        "name": "Data access",
        "description": "Endpoints for fetching data from the API, either related to "
        "individual entities, or for bulk data access in various forms.",
        "externalDocs": {
            "description": "Data dictionary",
            "url": "https://opensanctions.org/reference/",
        },
    },
    {
        "name": "Reconciliation",
        "description": "The Reconciliation Service provides four separate endpoints "
        "that work in concert to implement the data matching API used by OpenRefine, "
        "Wikidata and several other services and utilities.",
        "externalDocs": {
            "description": "W3C Community API specification",
            "url": "https://reconciliation-api.github.io/specs/latest/",
        },
    },
]

# Check if we're running in the context of unit tests:
TESTING = False
DEBUG = as_bool(env_str("YENTE_DEBUG", "false"))

MANIFEST_DEFAULT_PATH = Path(__file__).parent.parent / "manifests/default.yml"
MANIFEST = env_str("YENTE_MANIFEST", MANIFEST_DEFAULT_PATH.as_posix())
CRON: Optional[Cron] = None
CRONTAB = env_str("YENTE_CRONTAB", random_cron())
AUTO_REINDEX = as_bool(env_str("YENTE_AUTO_REINDEX", "true"))
STREAM_LOAD = as_bool(env_str("YENTE_STREAM_LOAD", "true"))
DEFAULT_ALGORITHM = env_str("YENTE_DEFAULT_ALGORITHM", "logic-v1")
BEST_ALGORITHM = env_str("YENTE_BEST_ALGORITHM", "logic-v1")

DATA_PATH = Path(env_str("YENTE_DATA_PATH", "/tmp"))
RESOURCES_PATH = Path(__file__).parent.joinpath("resources")

BASE_SCHEMA = "Thing"
PORT = int(env_str("YENTE_PORT", env_str("PORT", "8000")))
UPDATE_TOKEN = env_str("YENTE_UPDATE_TOKEN", "unsafe-default")
CACHE_HEADERS = {
    "Cache-Control": "public; max-age=3600",
    "X-Robots-Tag": "none",
}

# Set a proxy for outgoing HTTP requests:
HTTP_PROXY = env_str("YENTE_HTTP_PROXY", "")

# How many results to return per page of search results max:
MAX_PAGE = 500

# How many entities to accept in a /match batch at most:
MAX_BATCH = int(env_str("YENTE_MAX_BATCH", "100"))
MAX_RESULTS = 9999
MAX_OFFSET = MAX_RESULTS - MAX_PAGE

# How many results to return per /match query by default:
MATCH_PAGE = int(env_str("YENTE_MATCH_PAGE", "5"))

# How many results to return per /match query at most:
MAX_MATCHES = int(env_str("YENTE_MAX_MATCHES", "10"))

# How many candidates to retrieve as a multiplier of the /match limit:
MATCH_CANDIDATES = int(env_str("YENTE_MATCH_CANDIDATES", "10"))

# Whether to run expensive levenshtein queries inside ElasticSearch:
MATCH_FUZZY = as_bool(env_str("YENTE_MATCH_FUZZY", "true"))

# How many match and search queries to run against ES in parallel:
QUERY_CONCURRENCY = int(env_str("YENTE_QUERY_CONCURRENCY", "10"))

# Default scoring threshold for /match results:
SCORE_THRESHOLD = 0.70

# Default cutoff for scores that should not be returned as /match results:
SCORE_CUTOFF = 0.50

# ElasticSearch settings:
ES_URL = env_str("YENTE_ELASTICSEARCH_URL", "http://localhost:9200")
ES_USERNAME = env_get("YENTE_ELASTICSEARCH_USERNAME")
ES_PASSWORD = env_get("YENTE_ELASTICSEARCH_PASSWORD")
ES_CLOUD_ID = env_get("YENTE_ELASTICSEARCH_CLOUD_ID")
ES_SNIFF = as_bool(env_str("YENTE_ELASTICSEARCH_SNIFF", "false"))
ES_CA_CERT = env_get("YENTE_ELASTICSEARCH_CA_PATH")
ES_INDEX = env_str("YENTE_ELASTICSEARCH_INDEX", "yente")
ES_SHARDS = int(env_str("YENTE_ELASTICSEARCH_SHARDS", "1"))
ENTITY_INDEX = f"{ES_INDEX}-entities"
INDEX_VERSION = env_str("YENTE_INDEX_VERSION", "009")
INDEX_EXISTS_ABORT = as_bool(env_str("YENTE_INDEX_EXISTS_ABORT", "false"))

# Log output can be formatted as JSON:
LOG_JSON = as_bool(env_str("YENTE_LOG_JSON", "false"))
LOG_LEVEL = logging.DEBUG if DEBUG else logging.INFO

# Used to pad out first_seen, last_seen on static collections
RUN_TIME = datetime.utcnow().isoformat()[:19]

# Authentication settings
AUTH_TOKEN = env_get("YENTE_AUTH_TOKEN")
