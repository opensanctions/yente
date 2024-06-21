from .conftest import client

EXAMPLE = {
    "schema": "Person",
    "properties": {
        "name": ["Vladimir Putin"],
        "birthDate": ["1952"],
        "country": "Russia",
    },
}

ERMAKOV = {
    "properties": {
        "name": [
            "ERMAKOV Valery Nikolaevich",
            "Ermacov Valeryi Nycolaevych",
            "Ermakov Valerij Nikolaevich",
            "Ermakov Valerij Nikolaevič",
            "Ermakov Valerijj Nikolaevich",
            "Ermakov Valeriy Nikolaevich",
            "Ermakov Valery Nykolaevych",
            "Ermakov Valeryi Nykolaevych",
            "Ermakov Valeryy Nikolaevich",
            "Ermakov Valeryy Nykolaevych",
            "Ermakov Valerȳĭ Nȳkolaevȳch",
            "Iermakov Valerii Mykolaiovych",
            "Jermakov Valerij Mikolajovich",
            "Jermakov Valerij Mikolajovič",
            "Jermakov Valerij Mykolajovyč",
            "Yermakov Valerii Mykolaiovych",
            "Yermakov Valerij Mykolajovych",
            "Yermakov Valeriy Mykolayovych",
            "Êrmakov Valerìj Mikolajovič",
            "ЕРМАКОВ Валерий Николаевич",
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


def test_match_ermakov():
    query = {"queries": {"ermakov": ERMAKOV}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    results = resp.json()["responses"]["ermakov"]["results"]
    assert len(results) > 0, results

    params = {"fuzzy": "false"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    results2 = resp.json()["responses"]["ermakov"]["results"]
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


def test_filter_topic():
    query = {"queries": {"vv": EXAMPLE}}
    params = {"algorithm": "name-based", "topics": "crime.cyber"}
    resp = client.post("/match/default", json=query, params=params)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert len(res["results"]) == 0, res


def test_id_pass_through():
    body = dict(ERMAKOV)
    body["id"] = "ermakov"
    query = {"queries": {"no1": body}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["no1"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["id"] == "ermakov"
