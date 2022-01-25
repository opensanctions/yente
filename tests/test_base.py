from fastapi.testclient import TestClient

from yente.app import app

client = TestClient(app)


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


def test_healthz_again():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


def test_search():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results) == 0, results
