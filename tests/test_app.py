import json
import pytest

from fastapi.testclient import TestClient

from yente import settings
from yente.app import create_app
from .conftest import client

EXAMPLE = {
    "schema": "Person",
    "properties": {"name": ["Vladimir Putin"]},
}


@pytest.fixture(scope="session", autouse=True)
def load_data():
    """Shadow the ES-requiring load_data fixture from conftest for this module.
    Tests here that need indexed data must request it explicitly."""
    yield


def test_redoc_has_sri():
    """ReDoc docs page must reference the versioned URL and include SRI attributes.
    GET / is pure HTML with no Elasticsearch dependency."""
    c = TestClient(create_app())
    res = c.get("/")
    assert res.status_code == 200
    html = res.text
    assert f'src="{settings.REDOC_JS_URL}"' in html, "ReDoc script tag missing"
    assert f'integrity="{settings.REDOC_JS_SRI}"' in html, "SRI integrity attribute missing"
    assert 'crossorigin="anonymous"' in html, "crossorigin attribute missing"


def test_match_without_content_type_header():
    """Test that match requests work without a Content-Type header.

    This verifies that strict_content_type=False is set on app init,
    which was introduced to avoid breaking customers who don't send the header.
    """
    query = {"queries": {"q1": EXAMPLE}}
    res = client.post("/match/default", content=json.dumps(query))
    # Not 422 Unprocessable Entity
    assert res.status_code == 200, res.json()
