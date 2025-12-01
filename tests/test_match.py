from unittest import mock

from .conftest import client

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
            "Boris Borisovich ROTENBERG",
            "Boris Borissowitsch Rotenberg",
            "Борис Борисович Ротенберг",
            "Борис Ротенберг",
            "Ротенберг Борис Борисович",
            "Boris Borisovich Rotenberg ",
            "Rotenberh Borys Borysovych",
            "Ротенберг Борис Борисович",
            "Борис Борисович Ротенберг",
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


def test_match_no_schema():
    query = {"queries": {"fail": {"properties": {"name": "Banana"}}}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 422, resp.text

    query = {"queries": {"fail": {"schema": "xxx", "properties": {"name": "Banana"}}}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 400, resp.text


def test_match_many_names():
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
        mock_compare.return_value.features = {}
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
    query = {
        "queries": {"a": {"schema": "Person", "properties": {"name": "Viadimit Putln"}}}
    }

    with mock.patch("yente.settings.MATCH_FUZZY", False):
        resp = client.post(
            "/match/default", json=query, params={"algorithm": "logic-v1"}
        )
        data = resp.json()
        res = data["responses"]["a"]
        assert len(res["results"]) == 0, res

    with mock.patch("yente.settings.MATCH_FUZZY", True):
        # The result scores quite low, so we need to set a lower threshold to get a result
        resp = client.post(
            "/match/default",
            params={"threshold": 0.2, "algorithm": "logic-v1"},
            json=query,
        )
        data = resp.json()
        res = data["responses"]["a"]
        assert len(res["results"]) > 0, res
        assert res["results"][0]["id"] == "Q7747", res["results"][0]
        assert res["results"][0]["score"] > 0.5, res["results"][0]


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
