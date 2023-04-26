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


def test_match_putin_ofac_mode():
    query = {"queries": {"vv": EXAMPLE}}
    resp = client.post("/match/default", json=query, params={"algorithm": "neural_net"})
    assert resp.status_code == 400, resp.text

    resp = client.post("/match/default", json=query, params={"algorithm": "ofac_249"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "Q7747", res0
    assert res0["score"] > 0.90, res0


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


def test_id_pass_through():
    body = dict(ERMAKOV)
    body["id"] = "ermakov"
    query = {"queries": {"no1": body}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    res = resp.json()["responses"]["no1"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["id"] == "ermakov"
