from .conftest import client

from yente import settings


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


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
