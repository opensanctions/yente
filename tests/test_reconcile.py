import json
from normality import ascii_text

from .conftest import client


def test_reconcile_metadata():
    resp = client.get("/reconcile/default")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    url = "https://www.opensanctions.org"
    assert data["identifierSpace"].startswith(url), data
    assert len(data["defaultTypes"]) > 3, data
    assert "suggest" in data, data
    assert "extend" in data, data


def test_reconcile_post_query():
    queries = {"mutti": {"query": "Yevgeny Popov"}}
    resp = client.post("/reconcile/default", data={"queries": json.dumps(queries)})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["mutti"]["result"]
    assert res[0]["id"] == "Q18634850", res


def test_reconcile_post_extend():
    query = {"ids": ["Q7747"], "properties": [{"id": "name"}, {"id": "birthDate"}]}
    resp = client.post("/reconcile/default", data={"extend": json.dumps(query)})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["meta"]) == 2
    assert data["meta"][0]["id"] == "name", data["meta"]
    assert "Q7747" in data["rows"], data
    assert "name" in data["rows"]["Q7747"], data
    names = data["rows"]["Q7747"]["name"]
    assert len(names) > 0, names
    assert "putin" in "".join([n["str"] for n in names]).lower(), names


def test_reconcile_invalid():
    queries = {"mutti": {"type": "Banana"}}
    resp = client.post("/reconcile/default", data={"queries": json.dumps(queries)})
    assert resp.status_code == 400, resp.text


def test_reconcile_suggest_entity_no_prefix():
    resp = client.get("/reconcile/default/suggest/entity")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


def test_reconcile_suggest_entity_prefix():
    resp = client.get("/reconcile/default/suggest/entity?prefix=vladimir%20put")
    assert resp.status_code == 200, resp.text
    res = resp.json()["result"]
    assert len(res) > 0, res
    assert "Q7747" == res[0]["id"], res
    name = ascii_text(res[0]["name"])
    assert name is not None, res
    assert "vladimir" in name.lower(), name


def test_reconcile_suggest_entity_prefix_dummy():
    resp = client.get("/reconcile/default/suggest/entity?prefix=banana%20man")
    assert resp.status_code == 200, resp.text
    res = resp.json()["result"]
    assert len(res) == 0, res


def test_reconcile_suggest_property_no_prefix():
    resp = client.get("/reconcile/default/suggest/property")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


def test_reconcile_suggest_property_prefix():
    resp = client.get("/reconcile/default/suggest/property?prefix=country")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) > 1, data
    types = [r["id"] for r in res]
    assert "LegalEntity:mainCountry" in types, types
    assert "Thing:country" in types, types


def test_reconcile_suggest_property_prefix_dummy():
    resp = client.get("/reconcile/default/suggest/property?prefix=banana")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 0, data


def test_reconcile_suggest_type_no_prefix():
    resp = client.get("/reconcile/default/suggest/type")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


def test_reconcile_suggest_type_prefix():
    resp = client.get("/reconcile/default/suggest/type?prefix=organ")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 1, data
    assert res[0]["id"] == "Organization", data


def test_reconcile_suggest_type_prefix_dummy():
    resp = client.get("/reconcile/default/suggest/type?prefix=banana")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 0, data


def test_reconcile_extend_properties():
    resp = client.get("/reconcile/default/extend/property?limit=5&type=LegalEntity")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "type" in data
    assert data["type"] == "LegalEntity", data
    assert data["limit"] == 5, data
    props = data["properties"]
    assert len(props) == 5
    ids = [p["id"] for p in props]
    assert "name" in ids
    assert "country" in ids


def test_reconcile_extend_properties_invalid_type():
    resp = client.get("/reconcile/default/extend/property?limit=5&type=Banana")
    assert resp.status_code == 400, resp.text
