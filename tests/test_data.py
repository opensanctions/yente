import json
from pathlib import Path

import pytest
from followthemoney import model
from followthemoney.names import entity_names
from pydantic import ValidationError
from rigour.names import NamePartTag

import yente.data
from yente import settings
from yente.data import get_catalog, refresh_catalog
from yente.data.dataset import Dataset
from yente.data.loader import load_json_lines
from yente.data.manifest import Catalog, Manifest, check_dataset_queries
from yente.data.util import expand_dates, get_url_local_path
from yente.exc import YenteConfigError

from .conftest import patch_catalog_response


def _resource_dataset_spec(checksum: str) -> dict:
    return {
        "name": "test_dataset",
        "title": "Test Dataset",
        "resource_name": "entities.ftm.json",
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": "https://example.com/entities.ftm.json",
                "checksum": checksum,
                "mime_type": "application/json+ftm",
            }
        ],
    }


def test_dataset_captures_checksum_when_verify_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "VERIFY_CHECKSUM", True)
    dataset = Dataset(_resource_dataset_spec("abc123"))
    assert dataset.model.entities_url == "https://example.com/entities.ftm.json"
    assert dataset.model.entities_checksum == "abc123"


def test_dataset_skips_checksum_when_verify_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "VERIFY_CHECKSUM", False)
    dataset = Dataset(_resource_dataset_spec("abc123"))
    assert dataset.model.entities_url == "https://example.com/entities.ftm.json"
    assert dataset.model.entities_checksum is None


@pytest.mark.asyncio
@pytest.mark.usefixtures("zala_test_dataset")
async def test_manifest():
    catalog = await get_catalog()
    assert len(catalog.datasets), catalog.datasets


@pytest.mark.asyncio
async def test_manifest_with_auth_token():
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
@pytest.mark.usefixtures("parteispenden_test_dataset")
async def test_local_dataset():
    catalog = await get_catalog()
    ds = catalog.require("parteispenden")
    assert ds.model.load
    assert ds.model.entities_url is not None
    assert "entities.ftm.json" in ds.model.entities_url
    lines = []
    async for line in load_json_lines(ds.model.entities_url, "test"):
        lines.append(line)
    assert len(lines) > 10, lines


@pytest.mark.asyncio
@pytest.mark.usefixtures("parteispenden_test_dataset")
async def test_catalog_and_local_dataset():
    """Test that a datasets and catalog datasets are both loaded into the internal catalog."""
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


@pytest.mark.asyncio
async def test_catalog_with_invalid_scope():
    catalog_response_data = {"datasets": [{"name": "eu_fsf", "title": "EU FSF"}]}
    with patch_catalog_response(catalog_response_data), pytest.raises(YenteConfigError):
        await Catalog.load(
            manifest=Manifest.model_validate(
                {
                    "catalogs": [
                        {
                            "url": "https://data.opensanctions.org/datasets/latest/index.json",
                            "scope": "invalid",
                        }
                    ],
                }
            )
        )


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


@pytest.mark.parametrize("datasets", ["alpha|", {"xor": ["alpha"]}, []])
def test_manifest_filter_profile_invalid_datasets(datasets):
    with pytest.raises(ValidationError, match="Invalid dataset query"):
        Manifest.model_validate(
            {"filter_profiles": {"profile1": {"datasets": datasets}}}
        )


def test_manifest_match_and_filter_profiles():
    manifest = Manifest.model_validate(
        {
            "match_profiles": {"strict": {"weights": {"weak_alias_match": 0.0}}},
            "filter_profiles": {
                "peps": {"datasets": "alpha | beta", "topics": ["role.pep"]}
            },
        }
    )
    assert manifest.match_profiles["strict"].weights == {"weak_alias_match": 0.0}
    assert manifest.match_profiles["strict"].config == {}
    # String form is parsed (and validated)
    assert manifest.filter_profiles["peps"].datasets == {"or": ["alpha", "beta"]}

    manifest = Manifest.model_validate(
        {"filter_profiles": {"profile1": {"datasets": {"or": ["alpha", "beta"]}}}}
    )
    assert manifest.filter_profiles["profile1"].datasets == {"or": ["alpha", "beta"]}

    manifest = Manifest.model_validate(
        {"filter_profiles": {"profile1": {"datasets": "alpha"}}}
    )
    assert manifest.filter_profiles["profile1"].datasets == "alpha"

    with pytest.raises(ValidationError):
        Manifest.model_validate({"match_profiles": {"strict": {"weight": {}}}})


def test_manifest_default_datasets():
    assert Manifest.model_validate({}).default_datasets is None
    manifest = Manifest.model_validate({"default_datasets": "alpha | beta"})
    assert manifest.default_datasets == {"or": ["alpha", "beta"]}
    with pytest.raises(ValidationError, match="Invalid dataset query"):
        Manifest.model_validate({"default_datasets": "alpha|"})


DATASETS = [{"name": "alpha", "title": "Alpha"}, {"name": "beta", "title": "Beta"}]


@pytest.mark.asyncio
async def test_check_dataset_queries():
    manifest = Manifest.model_validate(
        {
            "datasets": DATASETS,
            "default_datasets": "alpha | beta",
            "filter_profiles": {"betas": {"datasets": ["beta"]}, "pep": {}},
        }
    )
    check_dataset_queries(manifest, await Catalog.load(manifest))


@pytest.mark.asyncio
async def test_check_dataset_queries_implicit_default():
    # There's no `default` collection, and no default_datasets to check.
    manifest = Manifest.model_validate({"datasets": DATASETS})
    check_dataset_queries(manifest, await Catalog.load(manifest))


@pytest.mark.parametrize(
    "config,path",
    [
        pytest.param(
            {"default_datasets": "alpha | nope"}, "default_datasets", id="default"
        ),
        pytest.param(
            {"default_datasets": "default"}, "default_datasets", id="explicit"
        ),
        pytest.param(
            {"filter_profiles": {"s": {"datasets": ["nope"]}}},
            "filter_profiles.s.datasets",
            id="filter-profile",
        ),
    ],
)
@pytest.mark.asyncio
async def test_check_dataset_queries_unknown_dataset(config, path):
    manifest = Manifest.model_validate({"datasets": DATASETS, **config})
    catalog = await Catalog.load(manifest)
    with pytest.raises(YenteConfigError, match=f"Invalid {path} in the manifest"):
        check_dataset_queries(manifest, catalog)


@pytest.mark.asyncio
async def test_refresh_keeps_valid_manifest(monkeypatch, tmp_path):
    monkeypatch.setattr(yente.data, "_manifest", None)
    monkeypatch.setattr(yente.data, "_catalog", None)
    path = tmp_path / "manifest.yml"
    monkeypatch.setattr(settings, "MANIFEST", str(path))

    path.write_text(json.dumps({"datasets": DATASETS, "default_datasets": "alpha"}))
    await refresh_catalog()
    manifest = yente.data._manifest
    assert manifest is not None
    assert manifest.default_datasets == "alpha"

    path.write_text(json.dumps({"datasets": DATASETS, "default_datasets": "nope"}))
    await refresh_catalog()
    assert yente.data._manifest is manifest
