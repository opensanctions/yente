import pytest
from unittest.mock import MagicMock, patch
from httpx import HTTPStatusError
from yente import settings

from yente.search.indexer import delta_index, first_available_delta
from yente.search.base import Index, SearchProvider, get_current_version

# TODO: Mock httpx instead
DS_WITH_DELTAS = "https://data.opensanctions.org/artifacts/sanctions/versions.json"


@pytest.mark.asyncio
@patch("yente.data.get_catalog")
@patch("yente.data.manifest.Catalog")
@patch("yente.data.manifest.Dataset")
async def test_delta_index_version(MockDataset, MockCatalog, get_catalog_mock):
    c = MockCatalog()
    has_version = MockDataset()
    c.datasets = [has_version]
    get_catalog_mock.return_value = c
    has_version.delta_index = DS_WITH_DELTAS
    # When passed a dataset that implements a version file it should be returned
    async for res in delta_index():
        assert res is not None
    # When passed a dataset that does not have deltas or does not exist an empty generator is returned
    no_versions = MockDataset()
    no_versions.delta_index = (
        "https://data.opensanctions.org/artifacts/no_such_dataset/versions.json"
    )
    c.datasets = [no_versions]
    async for res in delta_index():
        assert res is None


@pytest.mark.asyncio
@patch("yente.data.manifest.Dataset")
async def test_gets_the_current_index_version(MockDataset):
    # Given a dataset with a version
    m = MockDataset()
    m.name = "sanctions"
    m.version = 1
    provider = await SearchProvider.create()
    index = Index(provider, dataset=m)
    await index.upsert()
    resp = await index.add_alias(settings.ENTITY_INDEX)
    assert resp.body["acknowledged"] is True
    # It should be possible the current version from indexed dataset
    version = await get_current_version(provider, m)
    assert version == f"{settings.INDEX_VERSION}{m.version}"
    # Updating the dataset should change the version
    m.version = 2
    index = Index(provider, dataset=m)
    await index.upsert()
    await index.add_alias(settings.ENTITY_INDEX)
    version = await get_current_version(provider, m)
    assert version == f"{settings.INDEX_VERSION}{m.version}"
    # But adding a different dataset should not change the version
    m2 = m.deepcopy()
    m2.name = "default"
    m2.version = 3
    index2 = Index(provider, dataset=m2)
    await index2.upsert()
    await index2.add_alias(settings.ENTITY_INDEX)
    version = await get_current_version(provider, m)
    assert version == f"{settings.INDEX_VERSION}{m.version}"


@pytest.mark.asyncio
@patch("yente.data.manifest.Dataset")
async def test_can_do_switchover(MockDataset):
    # Given two different datasets
    sanctions = MockDataset()
    sanctions.name = "sanctions"
    sanctions.version = 1
    default = sanctions.deepcopy()
    default.name = "default"
    default.version = 1
    provider = await SearchProvider.create()
    # Each of which have an index and sharing an alias
    sanctions_index = Index(provider, dataset=sanctions)
    default_index = Index(provider, dataset=default)
    await sanctions_index.upsert()
    await sanctions_index.add_alias(settings.ENTITY_INDEX)
    await default_index.upsert()
    await default_index.add_alias(settings.ENTITY_INDEX)
    # Both indexes should be aliased
    sources = await provider.get_alias_sources(settings.ENTITY_INDEX)
    assert sanctions_index.name in sources.keys()
    assert default_index.name in sources.keys()
    # When switching over one of the indexes
    updated_sanctions = sanctions.deepcopy()
    updated_sanctions.name = "sanctions"
    updated_sanctions.version = 2
    clone = await sanctions_index.clone(updated_sanctions)
    await clone.make_main()
    # Then the clone should be aliased and the original should not
    sources = await provider.get_alias_sources(settings.ENTITY_INDEX)
    assert clone.name in sources.keys()
    assert sanctions_index.name not in sources.keys()
    # And the non-cloned index should still be aliased
    assert default_index.name in sources.keys()
