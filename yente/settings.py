import logging
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional
from banal import as_bool
from os import environ as env
from normality import stringify
from datetime import datetime, timezone
from aiocron import Cron  # type: ignore
import random


def env_get(name: str) -> Optional[str]:
    """Ensure the env returns a string even on Windows (#100)."""
    return stringify(env.get(name))


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


def env_legacy(new_name: str, old_name: str, default: str) -> str:
    """Transition to a new environment variable name with a warning."""
    if old_name in env:
        msg = f"Environment variable {old_name} is deprecated, use {new_name} instead."
        warnings.warn(msg)
    return env_str(new_name, env_str(old_name, default))


def random_cron() -> str:
    """Randomize the minute of the cron schedule to avoid thundering herd problem."""
    random_minute = str(random.randint(0, 59))
    return f"{random_minute} * * * *"


VERSION = "4.2.3"
AUTHOR = "OpenSanctions"
HOME_PAGE = "https://www.opensanctions.org/"
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

# Turn on debug logging and other development features:
DEBUG = as_bool(env_str("YENTE_DEBUG", "false"))

MANIFEST_DEFAULT_PATH = Path(__file__).parent.parent / "manifests/default.yml"

# Path name for the manifest YAML file:
# see: https://www.opensanctions.org/docs/yente/datasets/
MANIFEST = env_str("YENTE_MANIFEST", MANIFEST_DEFAULT_PATH.as_posix())

# Authentication settings:
DATA_TOKEN = env_get("YENTE_DATA_TOKEN")

CRON: Optional[Cron] = None
CRONTAB = env_str("YENTE_CRONTAB", random_cron())

# Whether to automatically reindex the data in the background of the API process:
AUTO_REINDEX = as_bool(env_str("YENTE_AUTO_REINDEX", "true"))

# Fetch the entire bulk data file before indexing into the search index:
STREAM_LOAD = as_bool(env_str("YENTE_STREAM_LOAD", "true"))
# this would be cached here:
DATA_PATH = Path(env_str("YENTE_DATA_PATH", "/tmp"))

# Set a proxy for outgoing HTTP requests:
HTTP_PROXY = env_str("YENTE_HTTP_PROXY", "")

# Whether to enable delta updates for the data:
DELTA_UPDATES = as_bool(env_str("YENTE_DELTA_UPDATES", "true"))

RESOURCES_PATH = Path(__file__).parent.joinpath("resources")

BASE_SCHEMA = "Thing"
PORT = int(env_str("YENTE_PORT", env_str("PORT", "8000")))
HOST = env_str("YENTE_HOST", env_str("HOST", "0.0.0.0"))
UPDATE_TOKEN = env_str("YENTE_UPDATE_TOKEN", "unsafe-default")

# Matcher defaults:
DEFAULT_ALGORITHM = env_str("YENTE_DEFAULT_ALGORITHM", "logic-v1")
BEST_ALGORITHM = env_str("YENTE_BEST_ALGORITHM", "logic-v1")

# How many results to return per page of search results max:
MAX_PAGE = 500

# How many entities to accept in a /search-type endpoint by default:
DEFAULT_PAGE = 10

# How many entities to accept in a /match batch at most:
MAX_BATCH = int(env_str("YENTE_MAX_BATCH", "100"))
MAX_RESULTS = 9999
MAX_OFFSET = MAX_RESULTS - MAX_PAGE

# How many results to return per /match query by default:
MATCH_PAGE = int(env_str("YENTE_MATCH_PAGE", "5"))

# How many results to return per /match query at most:
MAX_MATCHES = int(env_str("YENTE_MAX_MATCHES", "500"))

# How many candidates to retrieve as a multiplier of the /match limit:
MATCH_CANDIDATES = int(env_str("YENTE_MATCH_CANDIDATES", "10"))

# Whether to run expensive levenshtein queries inside ElasticSearch:
MATCH_FUZZY = as_bool(env_str("YENTE_MATCH_FUZZY", "true"))

# How many match and search queries to run against ES in parallel:
QUERY_CONCURRENCY = int(env_str("YENTE_QUERY_CONCURRENCY", "50"))

# Default scoring threshold for /match results:
SCORE_THRESHOLD = 0.70

# Default cutoff for scores that should not be returned as /match results:
SCORE_CUTOFF = 0.50

# ElasticSearch and OpenSearch settings:
INDEX_TYPE = env_str("YENTE_INDEX_TYPE", "elasticsearch").lower().strip()
if INDEX_TYPE not in ["elasticsearch", "opensearch"]:
    raise ValueError(f"Invalid index type: {INDEX_TYPE}")
_INDEX_URL = "http://localhost:9200"
INDEX_URL = env_legacy("YENTE_INDEX_URL", "YENTE_ELASTICSEARCH_URL", _INDEX_URL)

_INDEX_USERNAME = env_legacy("YENTE_INDEX_USERNAME", "YENTE_ELASTICSEARCH_USERNAME", "")
INDEX_USERNAME = None if _INDEX_USERNAME == "" else _INDEX_USERNAME
_INDEX_PASSWORD = env_legacy("YENTE_INDEX_PASSWORD", "YENTE_ELASTICSEARCH_PASSWORD", "")
INDEX_PASSWORD = None if _INDEX_PASSWORD == "" else _INDEX_PASSWORD
_INDEX_SNIFF = env_legacy("YENTE_INDEX_SNIFF", "YENTE_ELASTICSEARCH_SNIFF", "false")
INDEX_SNIFF = as_bool(_INDEX_SNIFF)
_INDEX_CA_CERT = env_legacy("YENTE_INDEX_CA_PATH", "YENTE_ELASTICSEARCH_CA_PATH", "")
INDEX_CA_CERT = None if _INDEX_CA_CERT == "" else _INDEX_CA_CERT
INDEX_SHARDS = int(env_legacy("YENTE_INDEX_SHARDS", "YENTE_ELASTICSEARCH_SHARDS", "1"))
INDEX_AUTO_REPLICAS = env_str("YENTE_INDEX_AUTO_REPLICAS", "0-all")
INDEX_NAME = env_legacy("YENTE_INDEX_NAME", "YENTE_ELASTICSEARCH_INDEX", "yente")
ENTITY_INDEX = f"{INDEX_NAME}-entities"
INDEX_VERSION = env_str("YENTE_INDEX_VERSION", "011")
assert len(INDEX_VERSION) == 3, "Index version must be 3 characters long."

# ElasticSearch-only options:
ES_CLOUD_ID = env_get("YENTE_ELASTICSEARCH_CLOUD_ID")

# OpenSearch-only options:
OPENSEARCH_REGION = env_get("YENTE_OPENSEARCH_REGION")
OPENSEARCH_SERVICE = env_get("YENTE_OPENSEARCH_SERVICE")

# Log output can be formatted as JSON:
LOG_JSON = as_bool(env_str("YENTE_LOG_JSON", "false"))
LOG_LEVEL = logging.DEBUG if DEBUG else logging.INFO

# Used to pad out first_seen, last_seen on static collections
RUN_DT = datetime.now(timezone.utc)
RUN_TIME = RUN_DT.isoformat()[:19]
