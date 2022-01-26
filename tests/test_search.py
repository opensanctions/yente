from .conftest import client


def test_search_putin():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results), results


def test_search_limit():
    res = client.get("/search/default?limit=0&q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results) == 0, results


def test_search_offset():
    res = client.get("/search/default?offset=100&q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results) == 0, results
