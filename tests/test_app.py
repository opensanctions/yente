import json

from fastapi.testclient import TestClient

from yente import settings
from yente.app import create_app
from .conftest import client

EXAMPLE = {
    "schema": "Person",
    "properties": {"name": ["Vladimir Putin"]},
}


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


def test_max_url_length_rejects_oversized_query():
    """A URL above the cap is rejected with 414 before it can reach a handler."""
    long_q = "a" * (settings.MAX_URL_LENGTH + 100)
    res = client.get(f"/search/default?q={long_q}")
    assert res.status_code == 414, res.text
    assert res.json() == {"detail": "Request URI too long"}


def test_max_url_length_allows_normal_query():
    """A regular query string is unaffected by the middleware."""
    res = client.get("/search/default?q=acme")
    # 200 if the index is populated, 503 if it isn't — either confirms the
    # request reached the search handler rather than being short-circuited.
    assert res.status_code in (200, 503), res.text
