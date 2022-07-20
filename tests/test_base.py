from .conftest import client

from yente import settings


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res
    assert "x-trace-id" in res.headers


def test_healthz_with_user():
    headers = {"Authorization": "Token BANANA"}
    res = client.get("/healthz", headers=headers)
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res
    assert "x-trace-id" in res.headers
    assert "x-user-id" in res.headers
    assert res.headers["x-user-id"] == "banana", res.headers


def test_readyz():
    res = client.get("/readyz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res
    assert "x-trace-id" in res.headers


def test_manifest():
    res = client.get("/manifest")
    assert res.status_code == 200, res
    data = res.json()
    assert "datasets" in data
    assert "schedule" in data


def test_updatez_get():
    res = client.get("/updatez")
    assert res.status_code == 405, res.text


def test_updatez_no_token_configured():
    before = settings.UPDATE_TOKEN
    settings.UPDATE_TOKEN = ""
    res = client.post(f"/updatez?token={before}")
    assert res.status_code == 403, res.text
    settings.UPDATE_TOKEN = before


def test_updatez_no_token():
    res = client.post(f"/updatez?sync=true")
    assert res.status_code == 403, res.text


def test_updatez_with_token():
    res = client.post(f"/updatez?token={settings.UPDATE_TOKEN}&sync=true")
    assert res.status_code == 200, res.text
