from .conftest import client


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


def test_healthz_again():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res
