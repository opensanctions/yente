from .conftest import client


def test_reconcile_suggest_type_no_prefix():
    resp = client.get("/reconcile/default/suggest/type")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    assert len(data["result"]) == 0, data


def test_reconcile_suggest_type_prefix():
    resp = client.get("/reconcile/default/suggest/type?prefix=compan")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 1, data
    assert res[0]["id"] == "Company", data


def test_reconcile_suggest_type_prefix_dummy():
    resp = client.get("/reconcile/default/suggest/type?prefix=banana")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "result" in data
    res = data["result"]
    assert len(res) == 0, data
