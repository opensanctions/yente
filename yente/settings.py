import logging
from pathlib import Path
from typing import Optional
from banal import as_bool
from os import environ as env
from normality import stringify
from datetime import datetime


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


VERSION = "1.6.3"
AUTHOR = "OpenSanctions"
HOME_PAGE = "https://www.opensanctions.org"
EMAIL = "info@opensanctions.org"
CONTACT = {"name": AUTHOR, "url": HOME_PAGE, "email": EMAIL}

TITLE = env_str("YENTE_TITLE") or "yente"
DESCRIPTION = """
The OpenSanctions Match-making API provides tools that help you determine if any
of the people or companies mentioned in your data are subject to international
sanctions, known to be involved in criminal activity, or if they are politically
exposed people.

**IMPORTANT: This open source API is intended to be operated on-premises in your
infrastructure. Read below on deploying your own instance.**

In this service, there is support for the following operations:

* A simple text-based search for interactive applications (``/search``),
* A query-by-example endpoint for KYC-style tasks (``/match``),
* Support for getting graph data for a particular entity (``/entities``),
* Support for the OpenRefine Reconciliation API (``/reconcile``).

The API uses JSON for data transfer and does not support authentication or access
control.

Further reading:

* [Install and deployment](https://github.com/opensanctions/yente/blob/main/README.md)
* Intro to the [entity data model](https://www.opensanctions.org/docs/entities/)
* Tutorial: [Using the matching API to do KYC-style checks](/articles/2022-02-01-matching-api/)
* [Data dictionary](https://opensanctions.org/reference/)
* Advanced: [statement-based data model](https://www.opensanctions.org/docs/statements/)
"""

TAGS = [
    {
        "name": "Matching",
        "description": "Services that enable driving a user-facing entity search or"
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
    },
    {
        "name": "Reconciliation",
        "description": "The Reconciliation Service provides four separate endpoints"
        "that work in concert to implement the data matching API used by OpenRefine, "
        "Wikidata and several other services and utilities. Point ",
        "externalDocs": {
            "description": "Community specification",
            "url": "https://reconciliation-api.github.io/specs/latest/",
        },
    },
]

# Check if we're running in the context of unit tests:
TESTING = False
DEBUG = as_bool(env_str("YENTE_DEBUG", "false"))

MANIFEST = Path(__file__).parent / "../manifests/default.yml"
MANIFEST = Path(env_str("YENTE_MANIFEST") or MANIFEST).resolve()
if not MANIFEST.is_file():
    raise RuntimeError("Manifest file does not exist: %s" % MANIFEST)
MANIFEST_CRONTAB = env_str("YENTE_MANIFEST_CRONTAB")

BASE_SCHEMA = "Thing"
STATEMENT_API = as_bool(env_str("YENTE_STATEMENT_API", "false"))
PORT = int(env_str("YENTE_PORT") or "8000")
UPDATE_TOKEN = env_str("YENTE_UPDATE_TOKEN", "unsafe-default")
CACHE_HEADERS = {
    "Cache-Control": "public; max-age=3600",
    "X-Robots-Tag": "none",
}
MAX_PAGE = 500
MAX_BATCH = 100
MAX_RESULTS = 9999
MAX_OFFSET = MAX_RESULTS - MAX_PAGE
MAX_MATCHES = 10
MATCH_PAGE = 5

SCORE_THRESHOLD = 0.70
SCORE_CUTOFF = 0.10

# ElasticSearch settings:
ES_URL = env_str("YENTE_ELASTICSEARCH_URL", "http://localhost:9200")
ES_USERNAME = env_str("YENTE_ELASTICSEARCH_USERNAME")
ES_PASSWORD = env_str("YENTE_ELASTICSEARCH_PASSWORD")
ES_CLOUD_ID = env_str("YENTE_ELASTICSEARCH_CLOUD_ID")
ES_INDEX = env_str("YENTE_ELASTICSEARCH_INDEX", "yente")
ES_SHARDS = int(env_str("YENTE_ELASTICSEARCH_SHARDS") or "1")
ENTITY_INDEX = f"{ES_INDEX}-entities"
STATEMENT_INDEX = f"{ES_INDEX}-statements"
INDEX_VERSION = "".join([v.zfill(2) for v in VERSION.split(".")])

LOG_JSON = as_bool(env_str("YENTE_LOG_JSON", "false"))
LOG_LEVEL = logging.DEBUG if DEBUG else logging.INFO

# Used to pad out first_seen, last_seen on static collections
RUN_TIME = datetime.utcnow().isoformat()[:19]
