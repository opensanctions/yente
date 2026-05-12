from unittest import mock

from .conftest import client, assert_entity_shape

EXAMPLE = {
    "schema": "Person",
    "properties": {
        "name": ["Vladimir Putin"],
        "birthDate": ["1952"],
        "country": "Russia",
    },
}

MANY_NAMES = {
    "properties": {
        "name": [
            "Boris Romanovich ROTENBERG",
            "Boris Borissowitsch Rotenberg",
            "Борис Романович Ротенберг",
            "Борис РОТЕНБЕРГ",
            "Борис Романович РОТЕНБЕРГ",
            "Rotenberg Boris Romanovich",
            "Rotenberg Boriss Romanovitsch",
        ]
    },
    "schema": "Person",
}


def test_match_putin():
    query = {"queries": {"vv": EXAMPLE, "xx": EXAMPLE, "zz": EXAMPLE}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "Q7747", res0
    assert_entity_shape(res0)


def test_match_putin_name_based_mode():
    query = {"queries": {"vv": EXAMPLE}}
    resp = client.post("/match/default", json=query, params={"algorithm": "neural-net"})
    assert resp.status_code == 400, resp.text

    resp = client.post("/match/default", json=query, params={"algorithm": "name-based"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "Q7747", res0
    assert res0["score"] > 0.70, res0
    assert_entity_shape(res0)


def test_match_no_schema():
    query = {"queries": {"fail": {"properties": {"name": "Banana"}}}}
    resp = client.post("/match/default", json=query)
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
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 400, resp.text


def test_match_many_names_logic_v1():
    query = {"queries": {"q": MANY_NAMES}}
    params = {"algorithm": "logic-v1"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results = resp.json()["responses"]["q"]["results"]
    assert len(results) > 0, results

    params = {"fuzzy": "false", "algorithm": "logic-v1"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results2 = resp.json()["responses"]["q"]["results"]
    assert len(results) == len(results2), results2


def test_match_many_names():
    query = {"queries": {"q": MANY_NAMES}}
    params = {"algorithm": "best"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results = resp.json()["responses"]["q"]["results"]
    assert len(results) > 0, results

    params = {"fuzzy": "false", "algorithm": "best"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results2 = resp.json()["responses"]["q"]["results"]
    assert len(results) == len(results2), results2


def test_match_exclude_dataset():
    query = {"queries": {"vv": EXAMPLE}}
    params = {"algorithm": "name-based", "exclude_dataset": "eu_fsf"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


def test_match_include_dataset():
    # When querying Putin
    query = {"queries": {"vv": EXAMPLE}}
    # Using only datasets that do not include Putin
    params = {
        "algorithm": "name-based",
        "include_dataset": ["ae_local_terrorists", "mx_governors"],
    }
    resp = client.post("/match/default", json=query, params=params)
    # We should get a succesful response
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    # And we should get no matches
    assert len(res["results"]) == 0, res
    # When using a dataset that includes Putin
    params = {
        "algorithm": "name-based",
        "include_dataset": ["eu_fsf", "ae_local_terrorists"],
    }
    resp = client.post("/match/default", json=query, params=params)
    data = resp.json()
    res = data["responses"]["vv"]
    # And we should get matches
    assert len(res["results"]) > 0, res
    # When we exclude the eu_fsf dataset
    params = {
        "algorithm": "name-based",
        "include_dataset": ["eu_fsf", "mx_governors", "ae_local_terrorists"],
        "exclude_dataset": "eu_fsf",
    }
    # We should get no matches
    resp = client.post("/match/default", json=query, params=params)
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


def test_match_logic_v2_with_algorithm_config():
    query = {"queries": {"vv": EXAMPLE}, "config": {"nm_number_mismatch": 0.4}}
    with mock.patch(
        "nomenklatura.matching.logic_v2.model.LogicV2.compare"
    ) as mock_compare:
        mock_compare.return_value = mock.MagicMock()
        mock_compare.return_value.score = 0.8
        mock_compare.return_value.explanations = {}

        resp = client.post(
            "/match/default", json=query, params={"algorithm": "logic-v2"}
        )

        # Check that the config is passed properly to the compare method
        assert mock_compare.called
        _, call_kwargs = mock_compare.call_args
        assert call_kwargs["config"].config.get("nm_number_mismatch") == 0.4

    assert resp.status_code == 200, resp.text

    query = {"queries": {"vv": EXAMPLE}, "config": {"invalid_option": 0.4}}
    resp = client.post("/match/default", json=query, params={"algorithm": "logic-v2"})
    assert resp.status_code == 400, resp.text


def test_filter_topic():
    query = {"queries": {"vv": EXAMPLE}}
    params = {"algorithm": "name-based", "topics": "crime.cyber"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


def test_id_pass_through():
    body = dict(MANY_NAMES)
    body["id"] = "rotenberg"
    query = {"queries": {"no1": body}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["no1"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["id"] == "rotenberg"


def test_fuzzy_names():
    """Test that fuzzy retrieval from the index works."""

    # With MATCH_FUZZY off, even a single-character typo on any name token should
    # prevent the entity from being retrieved.
    with mock.patch("yente.settings.MATCH_FUZZY", False):
        # Edit-1 typo on each token. Both tokens have to be typo'd, otherwise an
        # exact match on the untouched token alone is enough to retrieve Putin
        # even without fuzzy matching. The typos are also chosen so their
        # metaphones (FLTMT, PTNK) differ from those of the original ("Vladimir",
        # "Putin" → FLTMR, PTN), so the phonetic channel doesn't retrieve Putin
        # either.
        resp = client.post(
            "/match/default",
            json={
                "queries": {
                    "a": {
                        "schema": "Person",
                        "properties": {"name": "Vladimit Puting"},
                    }
                }
            },
            params={"algorithm": "best", "threshold": 0.2},
        )
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) == 0, res

        # Edit-2 typos on both tokens, again chosen so the metaphones (FTMT, PFM)
        # differ from the originals (FLTMR, PTN).
        resp = client.post(
            "/match/default",
            json={
                "queries": {
                    "a": {
                        "schema": "Person",
                        "properties": {"name": "Viadimit Pufim"},
                    }
                }
            },
            params={"algorithm": "best", "threshold": 0.2},
        )
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) == 0, res

    # With MATCH_FUZZY on, both queries should recover Putin. We're testing
    # Elasticsearch retrieval here, not the logic-v2 scoring algorithm — the
    # threshold is set low because logic-v2 penalizes typo'd inputs and the score
    # is not what this test is about.
    with mock.patch("yente.settings.MATCH_FUZZY", True):
        # Edit-1 typo on each token. Both tokens have to be typo'd, otherwise an
        # exact match on the untouched token alone is enough to retrieve Putin
        # even without fuzzy matching. The typos are also chosen so their
        # metaphones (FLTMT, PTNK) differ from those of the original ("Vladimir",
        # "Putin" → FLTMR, PTN), so the phonetic channel doesn't retrieve Putin
        # either.
        resp = client.post(
            "/match/default",
            json={
                "queries": {
                    "a": {
                        "schema": "Person",
                        "properties": {"name": "Vladimit Puting"},
                    }
                }
            },
            params={"algorithm": "logic-v2", "threshold": 0.2},
        )
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) > 0, res
        assert res["results"][0]["id"] == "Q7747", res["results"][0]

        # Edit-2 typos on both tokens, again chosen so the metaphones (FTMT, PFM)
        # differ from the originals (FLTMR, PTN). EXPECTED TO FAIL: the
        # NAME_PART_FUZZY_FIELD deletion neighborhood is generated at depth 1, so
        # only edit-distance-1 token typos are recovered. Extending the deletion
        # depth to 2 would make this case pass.
        resp = client.post(
            "/match/default",
            json={
                "queries": {
                    "a": {
                        "schema": "Person",
                        "properties": {"name": "Viadimit Pufim"},
                    }
                }
            },
            params={"algorithm": "logic-v2", "threshold": 0.2},
        )
        res = resp.json()["responses"]["a"]
        assert len(res["results"]) > 0, res
        assert res["results"][0]["id"] == "Q7747", res["results"][0]


def test_match_numeric_property_value():
    # Numeric values in a property list are coerced to strings by
    # extract_values; the request must succeed and echo the value back as a
    # string.
    query = {
        "queries": {
            "q": {
                "schema": "Person",
                "properties": {"name": ["Vladimir Putin"], "birthDate": [1952]},
            }
        }
    }
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["q"]
    assert "1952" in res["query"]["properties"].get("birthDate", []), res["query"]


def test_exclude_entity_ids():
    query = {"queries": {"q": EXAMPLE}}

    # First test: no exclusions should return Q7747 as first result
    resp = client.post("/match/default", json=query)
    assert resp.json()["responses"]["q"]["results"][0]["id"] == "Q7747"

    # Second test: exclude canonical ID Q7747
    resp = client.post(
        "/match/default", json=query, params={"exclude_entity_ids": ["Q7747"]}
    )
    assert len(resp.json()["responses"]["q"]["results"]) == 0

    # Third test: exclude referent ID gb-hmt-14196 (canonical ID is Q7747)
    resp = client.post(
        "/match/default", json=query, params={"exclude_entity_ids": ["gb-hmt-14196"]}
    )
    assert len(resp.json()["responses"]["q"]["results"]) == 0
