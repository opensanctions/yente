import itertools
import string
from unittest import mock

import pytest

from .conftest import assert_entity_shape, client

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
def test_match_name_clause_limit():
    names = [
        "token" + "".join(t)
        for t in itertools.islice(
            itertools.product(string.ascii_lowercase, repeat=3), 900
        )
    ]
    query = {
        "queries": {
            "large": {
                "schema": "Person",
                "properties": {"name": names},
            }
        }
    }
    with mock.patch("yente.routers.match.search_entities") as search:
        resp = client.post("/match/zala", json=query)
        search.assert_not_called()
    assert resp.status_code == 400, resp.text
    assert "requires 901 clauses; maximum is 900" in resp.json()["detail"]


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
    # A name with its spaces omitted is a single token to the name analysis, so no
    # per-part clause can reach the indexed name. The indexed space-less form of each
    # name (name_joined) bridges that exactly.
    query = {
        "queries": {
            "a": {
                "schema": "Person",
                "properties": {"name": ["alexandervyacheslavovichzakharov"]},
            }
        }
    }

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

    # The result scores low, so the threshold is lowered: this tests retrieval from
    # the index, not the scoring.
    resp = client.post(
        "/match/zala",
        params={"threshold": 0.2, "algorithm": "logic-v2"},
        json=query,
    )
    res = resp.json()["responses"]["a"]
    assert len(res["results"]) > 0
    assert res["results"][0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw"


@pytest.mark.usefixtures("zala_test_dataset")
def test_fuzzy_names_first_letter():
    # Edits at the first letter of both parts. A fuzzy clause with a fixed first
    # letter could not retrieve this; the deletion variants are position-blind.
    query = {
        "queries": {
            "a": {"schema": "Person", "properties": {"name": "Zlexander Sakharov"}}
        }
    }

    resp = client.post(
        "/match/zala",
        json=query,
        params={"algorithm": "logic-v2", "threshold": 0.2},
    )
    res = resp.json()["responses"]["a"]
    assert len(res["results"]) > 0
    assert res["results"][0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw"


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


@pytest.mark.usefixtures("zala_test_dataset")
def test_match_candidate_search_skips_total():
    import yente.routers.match as match_router

    with mock.patch.object(
        match_router, "search_entities", wraps=match_router.search_entities
    ) as spy:
        resp = client.post("/match/zala", json={"queries": {"q": QUERY_ZAKHAROV}})
    assert resp.status_code == 200, resp.text
    assert spy.call_args.kwargs["track_total_hits"] is False
    res = resp.json()["responses"]["q"]
    assert res["total"]["value"] == len(res["results"])
    assert len(res["results"]) > 0
