import pytest

from .conftest import (
    FIXTURES_PATH,
    build_index_alias_name_for_fixture,
    client,
    patch_catalog_response,
    patch_yente_catalog,
)

from yente import settings
from yente.data.manifest import Manifest
from yente.search.indexer import update_index


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


@pytest.mark.usefixtures("zala_test_dataset")
def test_readyz():
    res = client.get("/readyz")
    assert res.status_code == 200, res
    assert res.json().get("status") == "ok", res


@pytest.mark.usefixtures("zala_test_dataset")
def test_manifest():
    res = client.get("/manifest")
    assert res.status_code == 200, res
    data = res.json()
    assert "datasets" in data
    assert len(data["datasets"]) == 1


def test_algorithms(monkeypatch):
    # Don't hide logic-v2 for this test
    monkeypatch.setattr(settings, "HIDDEN_ALGORITHMS", [])
    res = client.get("/algorithms")
    assert res.status_code == 200, res
    data = res.json()
    assert "algorithms" in data
    assert len(data["algorithms"]) > 3

    # Ensure that the logic-v2 algorithm is visible and that
    # the configuration options are exposed
    logic_v2 = next((a for a in data["algorithms"] if a["name"] == "logic-v2"), None)
    assert logic_v2 is not None
    assert logic_v2["docs"] is not None
    assert logic_v2["docs"]["config"] is not None
    assert logic_v2["docs"]["config"]["nm_number_mismatch"] is not None


def test_algorithms_hidden(monkeypatch):
    res = client.get("/algorithms")
    visible_algorithms = [a["name"] for a in res.json()["algorithms"]]
    assert "logic-v1" in visible_algorithms

    monkeypatch.setattr(settings, "HIDDEN_ALGORITHMS", ["logic-v1"])
    res = client.get("/algorithms")
    visible_algorithms = [a["name"] for a in res.json()["algorithms"]]
    assert "logic-v1" not in visible_algorithms


@pytest.mark.asyncio
async def test_catalog(monkeypatch):
    monkeypatch.setattr(
        settings, "ENTITY_INDEX", build_index_alias_name_for_fixture("mocked_eu_fsf")
    )
    # Mocked HTTP response for the remote catalog. Includes:
    #   - eu_fsf: scoped/loaded below, so it gets indexed and shows up as `current`.
    #   - us_ofac_sdn: present in the catalog but NOT the loaded scope — covers
    #     the "non-loaded dataset in catalog" case (index_current=False).
    catalog_response = {
        "datasets": [
            {
                "name": "eu_fsf",
                "title": "EU FSF",
                "version": "20240101000000-aaa",
                "entities_url": (
                    FIXTURES_PATH / "dataset" / "parteispenden" / "entities.ftm.json"
                ).as_uri(),
            },
            {
                "name": "us_ofac_sdn",
                "title": "US OFAC SDN",
                "version": "20240101000000-bbb",
            },
        ]
    }
    # parteispenden is declared as a local dataset alongside the (mocked) remote
    # catalog to exercise the "catalog + local dataset" mix yente supports.
    manifest = Manifest.model_validate(
        {
            "catalogs": [
                {
                    "url": "https://catalog.example.com/index.json",
                    "scope": "eu_fsf",
                }
            ],
            "datasets": [
                {
                    "name": "parteispenden",
                    "title": "German political party donations",
                    "path": str(
                        FIXTURES_PATH
                        / "dataset"
                        / "parteispenden"
                        / "entities.ftm.json"
                    ),
                    "version": "100",
                }
            ],
        }
    )

    with patch_catalog_response(catalog_response):
        async with patch_yente_catalog(manifest):
            await update_index()
            res = client.get("/catalog")

    assert res.status_code == 200
    data = res.json()
    assert "current" in data
    assert "eu_fsf" in data["current"]
    assert "datasets" in data
    datasets = {d["name"]: d for d in data["datasets"]}
    # us_ofac_sdn is in the catalog but not the loaded scope.
    assert datasets["us_ofac_sdn"]["index_current"] is False
    assert datasets["eu_fsf"]["index_current"] is True
    # parteispenden is the local-dataset half of the mix.
    donations = datasets["parteispenden"]
    assert donations["load"] is True
    assert donations["index_current"] is True
    assert donations["index_version"] == "100"
    assert donations["version"] == "100"


@pytest.mark.asyncio
async def test_catalog_etag():
    manifest = Manifest.model_validate(
        {
            "datasets": [
                {
                    "name": "parteispenden",
                    "title": "German political party donations",
                    "path": str(
                        FIXTURES_PATH
                        / "dataset"
                        / "parteispenden"
                        / "entities.ftm.json"
                    ),
                    "version": "100",
                    "load": True,
                }
            ]
        }
    )
    async with patch_yente_catalog(manifest):
        res = client.get("/catalog")
        assert res.status_code == 200, res.text
        etag = res.headers.get("etag")
        assert etag is not None
        assert res.headers.get("cache-control") == "public, max-age=300"

        # A matching validator yields a bodiless 304 that still carries it.
        not_modified = client.get("/catalog", headers={"If-None-Match": etag})
        assert not_modified.status_code == 304, not_modified.text
        assert not_modified.headers.get("etag") == etag
        assert not_modified.content == b""

        # A stale validator still returns the full body.
        stale = client.get("/catalog", headers={"If-None-Match": '"stale"'})
        assert stale.status_code == 200
        assert stale.headers.get("etag") == etag

        # The wildcard matches the current representation.
        wildcard = client.get("/catalog", headers={"If-None-Match": "*"})
        assert wildcard.status_code == 304


def test_updatez_get():
    res = client.get("/updatez")
    assert res.status_code == 405, res.text


def test_updatez_no_token_configured(monkeypatch):
    monkeypatch.setattr(settings, "UPDATE_TOKEN", "")
    res = client.post(f"/updatez?token={settings.UPDATE_TOKEN}")
    assert res.status_code == 403, res.text


def test_updatez_no_token():
    res = client.post("/updatez?sync=true")
    assert res.status_code == 403, res.text


def test_updatez_with_token(monkeypatch):
    monkeypatch.setattr(settings, "UPDATE_TOKEN", "test")
    res = client.post(f"/updatez?token={settings.UPDATE_TOKEN}&sync=true")
    assert res.status_code == 200, res.text
