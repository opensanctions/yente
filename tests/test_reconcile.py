import json
import pytest
from normality import ascii_text

from .conftest import client


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_metadata():
    resp = client.get("/reconcile/zala")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    url = "https://www.opensanctions.org"
    assert data["identifierSpace"].startswith(url), data
    assert len(data["defaultTypes"]) > 3, data
    assert "suggest" in data, data
    assert "extend" in data, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_post_query():
    queries = {"mutti": {"query": "Alexander ZAKHAROV"}}
    resp = client.post("/reconcile/zala", data={"queries": json.dumps(queries)})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    res = data["mutti"]["result"]
    assert res[0]["id"] == "NK-aU5ybkbRFJucf8YMwsJvDw", res


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_post_extend():
    query = {
        "ids": ["NK-aU5ybkbRFJucf8YMwsJvDw"],
        "properties": [{"id": "name"}, {"id": "birthDate"}],
    }
    resp = client.post("/reconcile/zala", data={"extend": json.dumps(query)})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["meta"]) == 2
    assert data["meta"][0]["id"] == "name", data["meta"]
    assert "NK-aU5ybkbRFJucf8YMwsJvDw" in data["rows"], data
    assert "name" in data["rows"]["NK-aU5ybkbRFJucf8YMwsJvDw"], data
    names = data["rows"]["NK-aU5ybkbRFJucf8YMwsJvDw"]["name"]
    assert len(names) > 0, names
    assert "zakharov" in "".join([n["str"] for n in names]).lower(), names


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_invalid():
    queries = {"mutti": {"type": "Banana"}}
    resp = client.post("/reconcile/zala", data={"queries": json.dumps(queries)})
    assert resp.status_code == 400, resp.text


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_entity_no_prefix():
    resp = client.get("/reconcile/zala/suggest/entity")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_entity_prefix():
    resp = client.get("/reconcile/zala/suggest/entity?prefix=alexander%20zak")
    assert resp.status_code == 200, resp.text
    res = resp.json()["result"]
    assert len(res) > 0, res
    assert "NK-aU5ybkbRFJucf8YMwsJvDw" == res[0]["id"], res
    name = ascii_text(res[0]["name"])
    assert name is not None, res
    assert "aleksandr" in name.lower(), name


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_entity_prefix_dummy():
    resp = client.get("/reconcile/zala/suggest/entity?prefix=banana%20man")
    assert resp.status_code == 200, resp.text
    res = resp.json()["result"]
    assert len(res) == 0, res


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_property_no_prefix():
    resp = client.get("/reconcile/zala/suggest/property")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_property_prefix():
    resp = client.get("/reconcile/zala/suggest/property?prefix=country")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) > 1, data
    types = [r["id"] for r in res]
    assert "LegalEntity:mainCountry" in types, types
    assert "Thing:country" in types, types


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_property_prefix_dummy():
    resp = client.get("/reconcile/zala/suggest/property?prefix=banana")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 0, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_type_no_prefix():
    resp = client.get("/reconcile/zala/suggest/type")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_type_prefix():
    resp = client.get("/reconcile/zala/suggest/type?prefix=organ")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 1, data
    assert res[0]["id"] == "Organization", data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_suggest_type_prefix_dummy():
    resp = client.get("/reconcile/zala/suggest/type?prefix=banana")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 0, data


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_extend_properties():
    resp = client.get("/reconcile/zala/extend/property?limit=5&type=LegalEntity")
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


@pytest.mark.usefixtures("zala_test_dataset")
def test_reconcile_extend_properties_invalid_type():
    resp = client.get("/reconcile/zala/extend/property?limit=5&type=Banana")
    assert resp.status_code == 400, resp.text
