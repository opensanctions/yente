import os
import pytest
import requests
from urllib.parse import urljoin


DATASET = "default"


@pytest.fixture(scope="session")
def service_url() -> str:
    port = os.environ.get("YENTE_PORT", "8000")
    url = "http://localhost:%s" % port
    url = os.environ.get("YENTE_INTEGRATION_URL", url)
    return url


@pytest.fixture(scope="session")
def search_url(service_url) -> str:
    return urljoin(service_url, "/search/%s" % DATASET)


@pytest.fixture(scope="session")
def match_url(service_url) -> str:
    return urljoin(service_url, "/match/%s" % DATASET)


@pytest.fixture(scope="session")
def http() -> str:
    session = requests.Session()
    api_key = os.environ.get("YENTE_INTEGRATION_API_KEY", "none")
    session.headers["Authorization"] = f"ApiKey {api_key}"
    return session
