from .conftest import client

EXAMPLE = {
    "schema": "Person",
    "properties": {
        "name": ["Vladimir Putin"],
        "birthDate": ["1952"],
        "country": "Russia",
    },
}


def test_match_putin():
    query = {"queries": {"vv": EXAMPLE}}
    resp = client.post("/match/default", json=query)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["responses"]["vv"]
    assert res["query"]["schema"] == "Person"
    assert res["query"]["properties"]["country"][0] == "ru"
    assert res["total"]["value"] > 0, res["total"]
    res0 = res["results"][0]
    assert res0["id"] == "Q7747", res0
