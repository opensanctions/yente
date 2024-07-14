from datetime import datetime, timedelta

from .conftest import client


def test_search_putin():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results), results
    putin = results[0]
    assert putin["id"] == "Q7747", results
    assert putin["first_seen"] is not None, putin
    assert putin["first_seen"].startswith("20")
    assert putin["last_seen"] is not None, putin
    assert putin["last_seen"].startswith("20")
    assert "sanctions" not in putin["datasets"]
    assert "default" not in putin["datasets"]


def test_search_no_query():
    res = client.get("/search/default")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 9, results


def test_search_invalid_query():
    res = client.get("/search/default?q=invalid/query")
    assert res.status_code == 400, res
    res = client.get("/search/default?q=invalid/query&simple=true")
    assert res.status_code == 200, res


def test_search_missing_dataset():
    res = client.get("/search/banana")
    assert res.status_code == 404, res


def test_search_filter_schema_invalid():
    res = client.get("/search/default?q=angela merkel&schema=Banana")
    assert res.status_code == 400, res


def test_search_filter_schema_remove():
    res = client.get("/search/default?q=angela merkel&schema=Vessel")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


def test_search_filter_exclude_schema():
    res = client.get("/search/default?q=moscow")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 100, total
    res = client.get("/search/default?q=moscow&exclude_schema=Address")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total < total, new_total


def test_search_filter_exclude_dataset():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 0, total
    res = client.get("/search/default?q=vladimir putin&exclude_dataset=eu_fsf")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total == 0


def test_search_filter_include_dataset():
    res = client.get("/search/default?q=vladimir putin")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 0, total
    # When we include a dataset that does not contain Putin or is not available
    # in the collection we should get no results
    res = client.get("/search/default?q=vladimir putin&include_dataset=mx_senators")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total == 0
    # When we include a dataset that contains Putin we should get results
    res = client.get("/search/default?q=vladimir putin&include_dataset=eu_fsf")
    new_total = res.json()["total"]["value"]
    assert new_total > 0
    # When using both include and exclude, the exclude should take precedence
    res = client.get(
        "/search/default?q=vladimir putin&include_dataset=eu_fsf&exclude_dataset=eu_fsf"
    )
    new_total = res.json()["total"]["value"]
    assert new_total == 0


def test_search_filter_changed_since():
    ts = datetime.now() + timedelta(days=1)
    tx = ts.isoformat(sep="T", timespec="minutes")
    res = client.get(f"/search/default?q=vladimir putin&changed_since={tx}")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total == 0, total


def test_search_filter_schema_keep():
    res = client.get("/search/default?q=vladimir putin&schema=Person")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 0, results


def test_search_filter_countries_remove():
    res = client.get("/search/default?q=vladimir putin&countries=ke")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


def test_search_facet_datasets_default():
    res = client.get("/search/default")
    assert res.status_code == 200, res
    datasets = res.json()["facets"]["datasets"]
    names = [c["name"] for c in datasets["values"]]
    assert "eu_fsf" in names, names
    assert "parteispenden" not in names, names


def test_search_facet_datasets_spenden():
    res = client.get("/search/parteispenden")
    assert res.status_code == 200, res
    datasets = res.json()["facets"]["datasets"]
    names = [c["name"] for c in datasets["values"]]
    assert "eu_fsf" not in names, names
    assert "parteispenden" in names, names


def test_search_facet_countries():
    res = client.get("/search/default?q=vladimir putin&countries=ru")
    assert res.status_code == 200, res
    countries = res.json()["facets"]["countries"]
    names = [c["name"] for c in countries["values"]]
    assert "ru" in names, names
    assert "ke" not in names, names
    assert "lb" not in names, names


def test_search_facet_topics():
    res = client.get("/search/default?topics=sanction")
    assert res.status_code == 200, res
    sanctioned = res.json()["total"]["value"]
    assert sanctioned > 0, sanctioned

    res = client.get("/search/default")
    assert res.status_code == 200, res
    topics = res.json()["facets"]["topics"]
    names = [c["name"] for c in topics["values"]]
    assert "sanction" in names, names


def test_search_facet_schema():
    res = client.get("/search/default?schema=Address")
    assert res.status_code == 200, res
    addresses = res.json()["total"]["value"]
    assert addresses > 0, addresses

    res = client.get("/search/default?facets=schema")
    assert res.status_code == 200, res
    schemata = res.json()["facets"]["schema"]
    names = [c["name"] for c in schemata["values"]]
    assert "Address" in names, names


def test_search_facet_parameter():
    res = client.get("/search/default")
    assert res.status_code == 200, res
    facets = res.json()["facets"]
    assert len(list(facets.keys())) == 3
    assert "schema" not in facets
    assert "topics" in facets
    assert "datasets" in facets
    assert "countries" in facets

    res = client.get("/search/default?facets=schema&facets=topics")
    assert res.status_code == 200, res
    facets = res.json()["facets"]
    assert len(list(facets.keys())) == 2
    assert "schema" in facets
    assert "topics" in facets
    assert "datasets" not in facets
    assert "countries" not in facets


def test_search_no_targets():
    res = client.get("/search/default?schema=LegalEntity&target=false")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert not len(results), results
    # for res in results:
    #     assert res["target"] == False, res


def test_search_targets():
    res = client.get("/search/default?schema=LegalEntity&target=true")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results), results
    for res in results:
        assert res["target"] is True, res


def test_search_sorted():
    res = client.get("/search/default?sort=first_seen:desc")
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    prev_seen = None
    for res in results:
        if prev_seen is not None:
            assert res["first_seen"] <= prev_seen, res
        prev_seen = res["first_seen"]


def test_search_putin_scope():
    res = client.get("/search/peps?q=vladimir putin")
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
    assert data["offset"] == 100, data["offset"]


def test_search_range_offset():
    res = client.get("/search/default?offset=9999&q=putin")
    assert res.status_code == 422, res


def test_search_range_limit():
    res = client.get("/search/default?limit=10000&q=putin")
    assert res.status_code == 422, res

    res = client.get("/search/default?limit=500&q=putin")
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] < 10000, data
    assert data["offset"] == 0, data
