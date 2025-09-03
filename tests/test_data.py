import pytest
from pathlib import Path
from followthemoney import model
from rigour.names import NamePartTag

from yente.data import get_catalog
from yente.data.manifest import Catalog, Manifest
from yente.data.loader import load_json_lines
from yente.data.util import get_url_local_path
from yente.data.util import entity_names, expand_dates
from .conftest import patch_catalog_response


@pytest.mark.asyncio
async def test_manifest():
    catalog = await get_catalog()
    assert len(catalog.datasets), catalog.datasets


@pytest.mark.asyncio
async def test_manifest_with_auth_token():
    # Clear any cached catalog instance
    Catalog.instance = None

    catalog_response_data = {"datasets": []}

    with patch_catalog_response(catalog_response_data) as (
        mock_client,
        mock_client_constructor,
    ):
        await Catalog.load(
            manifest=Manifest.model_validate(
                {
                    "catalogs": [
                        {
                            "url": "https://delivery.example.com/datasets/latest/index.json",
                            "auth_token": "secretsecret",
                        }
                    ],
                }
            )
        )
        # Assert that AsyncClient was constructed with the correct auth parameter
        mock_client_constructor.assert_called_once()
        client_constructor_kwargs = mock_client_constructor.call_args.kwargs
        assert client_constructor_kwargs["auth"].auth_token == "secretsecret"

        mock_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_manifest_with_auth_token_env_expansion(monkeypatch):
    # Clear any cached catalog instance
    Catalog.instance = None

    monkeypatch.setenv("TEST_CATALOG_AUTH_TOKEN", "secretenv")

    catalog_response_data = {"datasets": []}

    with patch_catalog_response(catalog_response_data) as (
        mock_client,
        mock_client_constructor,
    ):
        await Catalog.load(
            manifest=Manifest.model_validate(
                {
                    "catalogs": [
                        {
                            "url": "https://delivery.example.com/datasets/latest/index.json",
                            "auth_token": "$TEST_CATALOG_AUTH_TOKEN",
                        }
                    ],
                }
            )
        )
        # Assert that AsyncClient was constructed with the correct auth parameter
        mock_client_constructor.assert_called_once()
        client_constructor_kwargs = mock_client_constructor.call_args.kwargs
        assert client_constructor_kwargs["auth"].auth_token == "secretenv"

        mock_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_local_dataset():
    catalog = await get_catalog()
    ds = catalog.require("parteispenden")
    assert ds.model.load
    assert ds.model.entities_url is not None
    assert "donations.ijson" in ds.model.entities_url
    lines = list()
    async for line in load_json_lines(ds.model.entities_url, "test"):
        lines.append(line)
    assert len(lines) > 10, lines


@pytest.mark.asyncio
async def test_catalog_and_local_dataset():
    """Test that a datasets and catalog datasets are both loaded into the internal catalog."""
    # Clear any cached catalog instance
    Catalog.instance = None

    catalog_response_data = {"datasets": [{"name": "eu_fsf", "title": "EU FSF"}]}

    with patch_catalog_response(catalog_response_data):
        catalog = await Catalog.load(
            manifest=Manifest.model_validate(
                {
                    "catalogs": [
                        {
                            # The response is mocked, so we can use any URL.
                            "url": "https://data.example.com/datasets/latest/index.json",
                        }
                    ],
                    "datasets": [
                        {
                            "name": "parteispenden",
                            "title": "German political party donations",
                        }
                    ],
                }
            )
        )

    assert len(catalog.datasets) == 2
    assert catalog.has("eu_fsf")
    assert catalog.has("parteispenden")


def test_get_url_local_path():
    out = get_url_local_path("http://banana.com/bla.txt")
    assert out is None
    out = get_url_local_path("https://banana.com/bla.txt")
    assert out is None
    out = get_url_local_path("file:///etc/passwd")
    assert isinstance(out, Path)
    assert "/etc/passwd" in out.as_posix()
    out = get_url_local_path("/etc/passwd")
    assert isinstance(out, Path)
    assert "/etc/passwd" in out.as_posix()
    out = get_url_local_path(__file__)
    assert isinstance(out, Path)

    with pytest.raises(RuntimeError):
        get_url_local_path("/no/such/path.csv")


def test_entity_names():
    person = model.make_entity("Person")
    person.add("name", "Vladimir Putin")
    person.add("name", "Влади́мир Влади́мирович ПУ́ТИН")
    person.add("name", "Влади́мир В. ПУ́ТИН")
    names = entity_names(person)
    for name in names:
        assert name.form in [
            "vladimir putin",
            "влади́мир влади́мирович пу́тин",
            "влади́мир в. пу́тин",
        ]
        for part in name.parts:
            assert part.form in [
                "vladimir",
                "putin",
                "путин",
                "влади́мир",
                "владимирович",
                "владимир",
                "в",
                "пу́тин",
            ]
            if part.form == "в":
                assert not part.metaphone

    company = model.make_entity("Company")
    company.add("name", "OAO Gazprom")
    for name in entity_names(company):
        assert name.form == "ao gazprom"
        for part in name.parts:
            if part.form == "ao":
                assert part.tag == NamePartTag.LEGAL


def test_expand_dates():
    dates = ["2023-01-01"]
    expanded = expand_dates(dates)
    assert len(expanded) == 3
    assert "2023" in expanded
    assert "2023-01" in expanded
    assert "2023-01-01" in expanded
    dates = ["2023-01-01", "2022"]
    expanded = expand_dates(dates)
    assert len(expanded) == 4
    assert "2022" in expanded
