import pytest

from .conftest import client, assert_entity_shape


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_zakharov():
    res = client.get("/search/zala?q=alexander zakharov")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results), results
    zakharov = results[0]
    assert_entity_shape(zakharov)
    assert zakharov["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw", results
    assert zakharov["first_seen"] is not None, zakharov
    assert zakharov["last_seen"] is not None, zakharov
    assert "sanctions" not in zakharov["datasets"]
    assert "default" not in zakharov["datasets"]


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_no_query():
    res = client.get("/search/zala")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 9, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_invalid_query():
    res = client.get("/search/zala?q=invalid/query")
    assert res.status_code == 400, res
    res = client.get("/search/zala?q=invalid/query&simple=true")
    assert res.status_code == 200, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_missing_dataset():
    res = client.get("/search/banana")
    assert res.status_code == 404, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_schema_invalid():
    res = client.get("/search/zala?q=angela merkel&schema=Banana")
    assert res.status_code == 400, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_schema_remove():
    res = client.get("/search/zala?q=angela merkel&schema=Vessel")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_exclude_schema():
    res = client.get("/search/zala?q=moscow")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 5
    res = client.get("/search/zala?q=moscow&exclude_schema=Address")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total < total, new_total


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_exclude_dataset():
    res = client.get("/search/zala?q=alexander zakharov")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 0, total
    res = client.get("/search/zala?q=alexander zakharov&exclude_dataset=zala")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total == 0


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_include_dataset():
    res = client.get("/search/zala?q=alexander zakharov")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total > 0, total
    # When we include a dataset that does not contain Alexander Zakharov or is not available
    # in the collection we should get no results
    res = client.get("/search/zala?q=alexander zakharov&include_dataset=mx_senators")
    assert res.status_code == 200, res
    new_total = res.json()["total"]["value"]
    assert new_total == 0
    # When we include a dataset that contains Alexander Zakharov we should get results
    res = client.get("/search/zala?q=alexander zakharov&include_dataset=zala")
    new_total = res.json()["total"]["value"]
    assert new_total > 0
    # When using both include and exclude, the exclude should take precedence
    res = client.get(
        "/search/zala?q=alexander zakharov&include_dataset=zala&exclude_dataset=zala"
    )
    new_total = res.json()["total"]["value"]
    assert new_total == 0


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_changed_since():
    tx = "2099-01-01T00:00"
    res = client.get(f"/search/zala?q=alexander zakharov&changed_since={tx}")
    assert res.status_code == 200, res
    total = res.json()["total"]["value"]
    assert total == 0, total


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_schema_keep():
    res = client.get("/search/zala?q=alexander zakharov&schema=Person")
    assert res.status_code == 200
    results = res.json()["results"]
    assert len(results) > 0


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_countries_remove():
    # Make sure the entity we're looking for is there
    res = client.get("/search/zala?q=alexander zakharov&countries=ru")
    assert res.status_code == 200
    assert len(res.json()["results"]) > 0

    # ... but they're not from Kenya
    res = client.get("/search/zala?q=alexander zakharov&countries=ke")
    assert res.status_code == 200
    assert len(res.json()["results"]) == 0


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter_countries_operator():
    res = client.get("/search/zala?q=alexander zakharov&countries=ke&countries=ru")
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) > 0, results

    res = client.get(
        "/search/zala?q=alexander zakharov&filter_op=AND&countries=ke&countries=ru"
    )
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_filter():
    res = client.get(
        "/search/zala?q=alexander zakharov&filter=properties.birthDate:1972-01-26§"
    )
    assert res.status_code == 200, res
    results = res.json()["results"]
    assert len(results) == 0, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_facet_datasets_default():
    res = client.get("/search/zala")
    assert res.status_code == 200, res
    datasets = res.json()["facets"]["datasets"]
    names = [c["name"] for c in datasets["values"]]
    assert "zala" in names, names
    assert "parteispenden" not in names, names


@pytest.mark.usefixtures("parteispenden_test_dataset")
def test_search_facet_datasets_spenden():
    res = client.get("/search/parteispenden")
    assert res.status_code == 200, res
    datasets = res.json()["facets"]["datasets"]
    names = [c["name"] for c in datasets["values"]]
    assert "eu_fsf" not in names, names
    assert "parteispenden" in names, names


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_facet_countries():
    res = client.get("/search/zala?q=alexander zakharov&countries=ru")
    assert res.status_code == 200
    countries = res.json()["facets"]["countries"]
    names = [c["name"] for c in countries["values"]]
    assert "ru" in names, names
    assert "ke" not in names, names
    assert "lb" not in names, names


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_facet_topics():
    res = client.get("/search/zala?topics=sanction")
    assert res.status_code == 200, res
    sanctioned = res.json()["total"]["value"]
    assert sanctioned > 0, sanctioned

    res = client.get("/search/zala")
    assert res.status_code == 200, res
    topics = res.json()["facets"]["topics"]
    names = [c["name"] for c in topics["values"]]
    assert "sanction" in names


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_facet_schema():
    res = client.get("/search/zala?schema=Address")
    assert res.status_code == 200, res
    addresses = res.json()["total"]["value"]
    assert addresses > 0, addresses

    res = client.get("/search/zala?facets=schema")
    assert res.status_code == 200, res
    schemata = res.json()["facets"]["schema"]
    names = [c["name"] for c in schemata["values"]]
    assert "Address" in names, names


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_facet_parameter():
    res = client.get("/search/zala")
    assert res.status_code == 200, res
    facets = res.json()["facets"]
    assert len(list(facets.keys())) == 3
    assert "schema" not in facets
    assert "topics" in facets
    assert "datasets" in facets
    assert "countries" in facets

    res = client.get("/search/zala?facets=schema&facets=topics")
    assert res.status_code == 200, res
    facets = res.json()["facets"]
    assert len(list(facets.keys())) == 2
    assert "schema" in facets
    assert "topics" in facets
    assert "datasets" not in facets
    assert "countries" not in facets


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_no_targets():
    res = client.get("/search/zala?schema=LegalEntity&target=true")
    assert res.status_code == 200
    assert len(res.json()["results"]) > 0

    res = client.get("/search/zala?schema=LegalEntity&target=false")
    assert res.status_code == 200, res
    assert len(res.json()["results"]) > 0


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_sorted():
    res = client.get("/search/zala?sort=first_seen:desc")
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    prev_seen = None
    for res in results:
        if prev_seen is not None:
            assert res["first_seen"] <= prev_seen
        prev_seen = res["first_seen"]


@pytest.mark.usefixtures("live_catalog_eu_fsf")
def test_search_zakharov_scope():
    res = client.get("/search/peps?q=alexander zakharov")
    assert res.status_code == 200, res
    data = res.json()
    results = data.get("results")
    assert len(results) == 0, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_limit():
    res = client.get("/search/zala?limit=0&q=alexander zakharov")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results) == 0, results


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_offset():
    res = client.get("/search/zala?offset=100&q=alexander zakharov")
    assert res.status_code == 200, res
    data = res.json()
    assert "results" in data, data
    results = data.get("results")
    assert len(results) == 0, results
    assert data["offset"] == 100, data["offset"]


def test_search_range_offset():
    res = client.get("/search/default?offset=9999&q=putin")
    assert res.status_code == 422, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_search_range_limit():
    res = client.get("/search/zala?limit=10000&q=zakharov")
    assert res.status_code == 422, res

    res = client.get("/search/zala?limit=500&q=zakharov")
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] < 10000, data
    assert data["offset"] == 0, data
