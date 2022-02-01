from .conftest import client


def test_search_putin():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results), results
    assert results[0]["id"] == "Q7747", results


def test_search_no_query():
    res = client.get("/search/default")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 9, results


def test_search_filter_schema_invalid():
    res = client.get("/search/default?q=angela merkel&schema=Banana")
    assert res.status_code == 400, res


def test_search_filter_schema_remove():
    res = client.get("/search/default?q=angela merkel&schema=Vessel")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


def test_search_filter_schema_keep():
    res = client.get("/search/default?q=angela merkel&schema=Person")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 0, results


def test_search_filter_countries_remove():
    res = client.get("/search/default?q=angela merkel&countries=ke")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


def test_search_facet_countries():
    res = client.get("/search/default?q=angela merkel&countries=de")
    assert res.status_code == 200, res
    countries = res.json()["facets"]["countries"]
    names = [c["name"] for c in countries["values"]]
    assert "de" in names, names
    assert "ke" not in names, names
    assert "ru" not in names, names


def test_search_putin_scope():
    res = client.get("/search/sanctions?q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    results = data.get("results")
    assert len(results) == 0, results


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
    assert data["offset"] == 1000, data["offset"]


def test_search_range_offset():
    res = client.get("/search/default?offset=100000&q=putin")
    assert res.status_code == 200, res
    data = res.json()
    assert data["offset"] < 10000, data
    assert data["limit"] == 0, data


def test_search_range_limit():
    res = client.get("/search/default?limit=100000&q=putin")
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] < 10000, data
    assert data["offset"] == 0, data
