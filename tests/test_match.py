import pytest
from unittest import mock

from .conftest import client, assert_entity_shape

QUERY_ZAKHAROV = {
    "schema": "Person",
    "properties": {
        "name": ["Alexander Vyacheslavovich ZAKHAROV"],
        "birthDate": ["1965"],
        "country": "Russia",
    },
}

MANY_NAMES = {
    "properties": {
        "name": [
            "Alexander Vyacheslavovich ZAKHAROV",
            "Aleksandr Vyacheslavovich Zakharov",
            "Александр Вячеславович Захаров",
            "Александр ЗАХАРОВ",
            "Захаров Александр Вячеславович",
            "Zakharov Aleksandr Vyacheslavovich",
            "Aleksandr Vjačeslavovič Zacharov",
        ]
    },
    "schema": "Person",
}


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_zakharov():
    query = {
        "queries": {"vv": QUERY_ZAKHAROV, "xx": QUERY_ZAKHAROV, "zz": QUERY_ZAKHAROV}
    }
    resp = client.post("/match/zala", json=query)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw", res0
    assert_entity_shape(res0)


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_zakharov_name_based_mode():
    query = {"queries": {"vv": QUERY_ZAKHAROV}}
    resp = client.post("/match/zala", json=query, params={"algorithm": "neural-net"})
    assert resp.status_code == 400, resp.text

    resp = client.post("/match/zala", json=query, params={"algorithm": "name-based"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw", res0
    assert res0["score"] > 0.70, res0
    assert_entity_shape(res0)


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_no_schema():
    query = {"queries": {"fail": {"properties": {"name": "Banana"}}}}
    resp = client.post("/match/zala", json=query)
    assert resp.status_code == 422, resp.text

    # Multi-query batch: an invalid schema in one query must surface a 400 to
    # the caller even when sibling queries are still in flight under
    # asyncio.gather.
    query = {
        "queries": {
            "fail": {"schema": "xxx", "properties": {"name": "Banana"}},
            "ok1": {"schema": "Person", "properties": {"name": ["Vladimir Putin"]}},
            "ok2": {"schema": "Person", "properties": {"name": ["John Doe"]}},
        }
    }
    resp = client.post("/match/zala", json=query)
    assert resp.status_code == 400, resp.text


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_many_names_logic_v1():
    query = {"queries": {"q": MANY_NAMES}}
    params = {"algorithm": "logic-v1"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results = resp.json()["responses"]["q"]["results"]
    assert len(results) > 0, results

    params = {"fuzzy": "false", "algorithm": "logic-v1"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results2 = resp.json()["responses"]["q"]["results"]
    assert len(results) == len(results2), results2


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_many_names():
    query = {"queries": {"q": MANY_NAMES}}
    params = {"algorithm": "best"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results = resp.json()["responses"]["q"]["results"]
    assert len(results) > 0, results

    params = {"fuzzy": "false", "algorithm": "best"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results2 = resp.json()["responses"]["q"]["results"]
    assert len(results) == len(results2), results2


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_exclude_dataset():
    query = {"queries": {"vv": QUERY_ZAKHAROV}}
    params = {"algorithm": "name-based", "exclude_dataset": "zala"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_include_dataset():
    # When querying Putin
    query = {"queries": {"vv": QUERY_ZAKHAROV}}
    # Using only datasets that do not include Putin
    params = {
        "algorithm": "name-based",
        "include_dataset": ["ae_local_terrorists", "mx_governors"],
    }
    resp = client.post("/match/zala", json=query, params=params)
    # We should get a succesful response
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    # And we should get no matches
    assert len(res["results"]) == 0, res
    # When using a dataset that includes Putin
    params = {
        "algorithm": "name-based",
        "include_dataset": ["zala", "ae_local_terrorists"],
    }
    resp = client.post("/match/zala", json=query, params=params)
    data = resp.json()
    res = data["responses"]["vv"]
    # And we should get matches
    assert len(res["results"]) > 0, res
    # When we exclude the eu_fsf dataset
    params = {
        "algorithm": "name-based",
        "include_dataset": ["zala", "mx_governors", "ae_local_terrorists"],
        "exclude_dataset": "zala",
    }
    # We should get no matches
    resp = client.post("/match/zala", json=query, params=params)
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_logic_v2_with_algorithm_config():
    query = {"queries": {"vv": QUERY_ZAKHAROV}, "config": {"nm_number_mismatch": 0.4}}
    with mock.patch(
        "nomenklatura.matching.logic_v2.model.LogicV2.compare"
    ) as mock_compare:
        mock_compare.return_value = mock.MagicMock()
        mock_compare.return_value.score = 0.8
        mock_compare.return_value.explanations = {}

        resp = client.post("/match/zala", json=query, params={"algorithm": "logic-v2"})

        # Check that the config is passed properly to the compare method
        assert mock_compare.called
        _, call_kwargs = mock_compare.call_args
        assert call_kwargs["config"].config.get("nm_number_mismatch") == 0.4

    assert resp.status_code == 200, resp.text

    query = {"queries": {"vv": QUERY_ZAKHAROV}, "config": {"invalid_option": 0.4}}
    resp = client.post("/match/zala", json=query, params={"algorithm": "logic-v2"})
    assert resp.status_code == 400, resp.text


@pytest.mark.usefixtures("zala_test_dataset")
def test_filter_topic():
    query = {"queries": {"vv": QUERY_ZAKHAROV}}
    params = {"algorithm": "name-based", "topics": "crime.cyber"}
    resp = client.post("/match/zala", json=query, params=params)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_id_pass_through():
    body = dict(MANY_NAMES)
    body["id"] = "zakharov"
    query = {"queries": {"no1": body}}
    resp = client.post("/match/zala", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["no1"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["id"] == "zakharov"


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_name_without_spaces():
    # A name with spaces omitted ("alexandervyacheslavovichzakharov") is a single token to
    # the query analyzer, so the per-token names match can't bridge the gap to the
    # separate indexed tokens. We solve this with the character n-gram sub-field on the
    # names field, which lets the space-less token still match the indexed name via shared
    # n-grams, and this test verifies that.
    #
    # The bogus "foo bar" name is just there to keep the query from being treated as a
    # single-word ("short") query, for which we always fall back to n-gram matching
    # regardless of the fuzzy setting. That way we can show the n-gram path is what makes
    # the space-less match work, by checking it disappears when fuzzy matching is off.
    query = {
        "queries": {
            "a": {
                "schema": "Person",
                "properties": {"name": ["alexandervyacheslavovichzakharov", "foo bar"]},
            }
        }
    }

    with mock.patch("yente.settings.MATCH_FUZZY", False):
        resp = client.post("/match/zala", json=query)
        assert resp.status_code == 200, resp.text
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) == 0

    with mock.patch("yente.settings.MATCH_FUZZY", True):
        resp = client.post("/match/zala", json=query)
        assert resp.status_code == 200, resp.text
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) > 0
        assert res["results"][0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw"


@pytest.mark.usefixtures("zala_test_dataset")
def test_fuzzy_names():
    """Test that fuzzy retrieval from the index works."""
    query = {
        "queries": {
            "a": {"schema": "Person", "properties": {"name": "Aiexandvr Zakharom"}}
        }
    }

    with mock.patch("yente.settings.MATCH_FUZZY", False):
        # We need to set a lower threshold to get logic-v2 to score it high enough.
        # That's okay, we care about testing the fuzzy retrieval from the index,
        # not the details of the scoring algorithm.
        resp = client.post(
            "/match/zala", json=query, params={"algorithm": "best", "threshold": 0.2}
        )
        data = resp.json()
        res = data["responses"]["a"]
        assert len(res["results"]) == 0, res

    with mock.patch("yente.settings.MATCH_FUZZY", True):
        # The result scores quite low, so we need to set a lower threshold to get a result
        resp = client.post(
            "/match/zala",
            params={"threshold": 0.2, "algorithm": "logic-v2"},
            json=query,
        )
        data = resp.json()
        res = data["responses"]["a"]
        assert len(res["results"]) > 0, res
        assert res["results"][0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw", res["results"][0]


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_numeric_property_value():
    # Numeric values in a property list are coerced to strings by
    # extract_values; the request must succeed and echo the value back as a
    # string.
    query = {
        "queries": {
            "q": {
                "schema": "Person",
                "properties": {"name": ["Alexander Zakharov"], "birthDate": [1965]},
            }
        }
    }
    resp = client.post("/match/zala", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["q"]
    assert "1965" in res["query"]["properties"].get("birthDate", []), res["query"]


@pytest.mark.usefixtures("zala_test_dataset")
def test_exclude_entity_ids():
    query = {"queries": {"q": QUERY_ZAKHAROV}}

    # First test: no exclusions should return NK-aU5ybkbRFJucf8YMwsJvDw as first result
    resp = client.post("/match/zala", json=query)
    assert (
        resp.json()["responses"]["q"]["results"][0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw"
    )

    # Second test: exclude canonical ID NK-aU5ybkbRFJucf8YMwsJvDw, should return no results
    resp = client.post(
        "/match/zala",
        json=query,
        params={"exclude_entity_ids": ["NK-aU5ybkbRFJucf8YMwsJvDw"]},
    )
    assert len(resp.json()["responses"]["q"]["results"]) == 0

    # Third test: exclude referent ID gb-hmt-14196 (canonical ID is NK-aU5ybkbRFJucf8YMwsJvDw)
    resp = client.post(
        "/match/zala", json=query, params={"exclude_entity_ids": ["ofac-45937"]}
    )
    assert len(resp.json()["responses"]["q"]["results"]) == 0
