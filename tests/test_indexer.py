import pytest
import pytest_asyncio

from yente import settings
from yente.data import get_catalog
from yente.data.manifest import Manifest
from yente.provider import SearchProvider
from yente.search.indexer import update_index
from yente.search.versions import build_index_name

from tests.conftest import (
    PARTEISPENDEN_MANIFEST,
    build_index_alias_name_for_fixture,
    patch_yente_catalog,
)


LAST_EXPORT_ISO = "2026-05-15T12:34:56"

_manifest_dict = PARTEISPENDEN_MANIFEST.model_dump(mode="json")
_manifest_dict["datasets"][0]["last_export"] = LAST_EXPORT_ISO
PARTEISPENDEN_WITH_LAST_EXPORT_MANIFEST = Manifest.model_validate(_manifest_dict)


@pytest_asyncio.fixture(scope="function")
async def parteispenden_with_last_export(monkeypatch):
    monkeypatch.setattr(
        settings,
        "ENTITY_INDEX",
        build_index_alias_name_for_fixture("parteispenden-last-export"),
    )
    async with patch_yente_catalog(PARTEISPENDEN_WITH_LAST_EXPORT_MANIFEST):
        await update_index()
        yield


@pytest.mark.asyncio
async def test_index_entities_writes_last_export_to_metadata(
    parteispenden_with_last_export,
    search_provider: SearchProvider,
):
    """index_entities should write dataset.model.last_export into the index
    `_meta` so it survives the load and can be read back."""
    catalog = await get_catalog()
    dataset = catalog.require("parteispenden")
    assert dataset.model.version is not None
    index = build_index_name(dataset.name, dataset.model.version)
    metadata = await search_provider.get_index_metadata(index)
    assert metadata.get("last_export") == dataset.model.last_export.isoformat()
