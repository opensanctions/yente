from .conftest import client

from yente import settings


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


def test_readyz():
    res = client.get("/readyz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


def test_manifest():
    res = client.get("/manifest")
    assert res.status_code == 200, res
    data = res.json()
    assert "datasets" in data
    assert len(data["datasets"]) > 5


def test_algorithms():
    res = client.get("/algorithms")
    assert res.status_code == 200, res
    data = res.json()
    assert "algorithms" in data
    assert len(data["algorithms"]) > 3


def test_catalog():
    res = client.get("/catalog")
    assert res.status_code == 200, res
    data = res.json()
    assert "current" in data
    assert "eu_fsf" in data["current"]
    assert "datasets" in data
    datasets = {d["name"]: d for d in data["datasets"]}
    assert datasets["us_ofac_sdn"]["index_current"] is False
    assert datasets["eu_fsf"]["index_current"] is True
    donations = datasets["parteispenden"]
    assert donations["load"] is True
    assert donations["index_current"] is True
    assert donations["index_version"] == "100"
    assert donations["version"] == "100"


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
    res = client.post("/updatez?sync=true")
    assert res.status_code == 403, res.text


def test_updatez_with_token():
    res = client.post(f"/updatez?token={settings.UPDATE_TOKEN}&sync=true")
    assert res.status_code == 200, res.text
