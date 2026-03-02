import json

from .conftest import client

EXAMPLE = {
    "schema": "Person",
    "properties": {"name": ["Vladimir Putin"]},
}


def test_match_without_content_type_header():
    """Test that match requests work without a Content-Type header.

    This verifies that strict_content_type=False is set on app init,
    which was introduced to avoid breaking customers who don't send the header.
    """
    query = {"queries": {"q1": EXAMPLE}}
    res = client.post("/match/default", content=json.dumps(query))
    # Not 422 Unprocessable Entity
    assert res.status_code == 200, res.json()
