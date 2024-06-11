import pytest
import json
from unittest.mock import patch, MagicMock
from .conftest import VERSIONS_PATH, FIXTURES_PATH

from yente import settings
from yente.search.indexer import (
    get_delta_versions,
    get_next_version,
    DeltasNotAvailable,
    get_deltas_from_version,
    delta_update_index,
)
from yente.search.base import Index, SearchProvider, get_current_version

# TODO: Mock httpx instead
DS_WITH_DELTAS = "https://data.opensanctions.org/artifacts/sanctions/versions.json"


@pytest.mark.asyncio
@patch("yente.data.manifest.Dataset")
async def test_getting_versions_from(MockDataset):
    with open(VERSIONS_PATH) as f:
        provider = await SearchProvider.create()
        dataset = MockDataset()
        dataset.delta_index = None
        # When the dataset does not have a version it should throw an error
        with pytest.raises(DeltasNotAvailable):
            await get_next_version(dataset, dataset.version)
        # When the dataset has a version not in the version index it should throw an Error
        versions = json.load(f).get("items")
        versions.sort()
        assert "0" not in versions  # doh
        dataset.version = "0"
        dataset.available_versions = MagicMock(return_value=versions)
        dataset.name = "foobar"
        dataset.delta_index = "file://" + VERSIONS_PATH.resolve().as_posix()
        index = Index(provider, dataset.name, dataset.version)
        await index.upsert()
        await index.add_alias(settings.ENTITY_INDEX)
        with pytest.raises(DeltasNotAvailable):
            await get_next_version(dataset, dataset.version)
        # When the dataset already has the newest version it should return None
        dataset.version = versions[-1]
        index = Index(provider, dataset.name, dataset.version)
        await index.upsert()
        await index.add_alias(settings.ENTITY_INDEX)
        assert await get_next_version(dataset, dataset.version) is None


@pytest.mark.asyncio
@patch("yente.data.manifest.Dataset")
@patch("yente.search.indexer.get_delta_version")
async def test_getting_deltas_from_version(
    get_delta_version_mock, MockDataset, httpx_mock
):
    ds = MockDataset()
    get_delta_version_mock.return_value = (
        "https://data.opensanctions.org/succesful/call/is/mocked"
    )

    # When the version has deltas it should return the deltas
    with open(FIXTURES_PATH / "entities.delta.json") as f:
        body = f.read()
    httpx_mock.add_response(200, content=body)
    res = get_deltas_from_version("has_deltas", ds)
    try:
        await res.__anext__()
    except StopAsyncIteration:
        pytest.fail("Expected deltas but got none")
    # When the path does not exist it should raise a DeltasNotAvailable error
    get_delta_version_mock.return_value = (
        "https://data.opensanctions.org/failing/call/is/mocked"
    )
    httpx_mock.add_response(
        404, url="https://data.opensanctions.org/failing/call/is/mocked"
    )
    res = get_deltas_from_version("no_deltas", ds)
    with pytest.raises(DeltasNotAvailable):
        await res.__anext__()


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
    async for res in get_delta_versions():
        assert res is not None
    # When passed a dataset that does not have deltas or does not exist an empty generator is returned
    no_versions = MockDataset()
    no_versions.delta_index = (
        "https://data.opensanctions.org/artifacts/no_such_dataset/versions.json"
    )
    c.datasets = [no_versions]
    async for res in get_delta_versions():
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
    version = await get_current_version(m, provider)
    assert version == f"{settings.INDEX_VERSION}{m.version}"
    # Updating the dataset should change the version
    m.version = 2
    index = Index(provider, dataset=m)
    await index.upsert()
    await index.add_alias(settings.ENTITY_INDEX)
    version = await get_current_version(m, provider)
    assert version == f"{settings.INDEX_VERSION}{m.version}"
    # But adding a different dataset should not change the version
    m2 = m.deepcopy()
    m2.name = "default"
    m2.version = 3
    index2 = Index(provider, dataset=m2)
    await index2.upsert()
    await index2.add_alias(settings.ENTITY_INDEX)
    version = await get_current_version(m, provider)
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


@pytest.mark.asyncio
async def test_end_to_end(httpx_mock):
    """
    Test getting the delta versions and updating the index, using the data
    mocks in the fixtures directory.
    """
    # No alias or index exists, so the first run should build the index from the beginning
    available_versions = json.loads((FIXTURES_PATH / "versions.json").read_text())
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/datasets/sanctions/entities.ftm.json",
        content=(FIXTURES_PATH / "dataset/has_deltas/entities.ftm.json").read_bytes(),
    )
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/artifacts/sanctions/versions.json",
        content=(FIXTURES_PATH / "start_version.json").read_bytes(),
    )
    await delta_update_index()
    for version, url in available_versions["versions"].items():
        httpx_mock.add_response(
            200,
            url=url,
            content=(
                FIXTURES_PATH / f"dataset/has_deltas/{version}/entities.delta.json"
            ).read_bytes(),
        )
    httpx_mock.add_response(
        200,
        url="https://data.opensanctions.org/artifacts/sanctions/versions.json",
        content=(FIXTURES_PATH / "versions.json").read_bytes(),
    )
    await delta_update_index()
