from banal import as_bool
from opensanctions.settings import env_str

VERSION = "1.0.0"
AUTHOR = "OpenSanctions"
HOME_PAGE = "https://www.opensanctions.org"
EMAIL = "info@opensanctions.org"
CONTACT = {"name": AUTHOR, "url": HOME_PAGE, "email": EMAIL}

TITLE = "yente"
DESCRIPTION = """
The OpenSanctions Match-making API provides tools that help you determine if any
of the people or companies mentioned in your data are subject to international
sanctions, known to be involved in criminal activity, or if they are politically
exposed people.

**IMPORTANT: This open source API is intended to be operated on-premises in your
infrastructure. The online version exists as a demo and does not provide any data
protection or uptime guarantees. Read below on deploying your own instance.**

In this service, there is support for the following operations:

* A simple text-based search for interactive applications (``/search``),
* A query-by-example endpoint for KYC-style tasks (``/match``),
* Support for getting graph data for a particular entity (``/entities``),
* Support for the OpenRefine Reconciliation API (``/reconcile``).

The API uses JSON for data transfer and does not support authentication or access
control.

Further reading:

* [Install and deployment](https://github.com/opensanctions/yente/blob/main/README.md)
* [Data dictionary](https://opensanctions.org/reference/)
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

DATA_INDEX = "https://data.opensanctions.org/datasets/latest/index.json"
DATA_INDEX = env_str("YENTE_DATA_INDEX", DATA_INDEX)
SCOPE_DATASET = env_str("YENTE_SCOPE_DATASET", "all")
ENDPOINT_URL = env_str("YENTE_ENDPOINT_URL", "http://localhost:8000")
ES_URL = env_str("YENTE_ELASTICSEARCH_URL", "http://localhost:9200")
ES_INDEX = env_str("YENTE_ELASTICSEARCH_INDEX", "yente")
BASE_SCHEMA = "Thing"
