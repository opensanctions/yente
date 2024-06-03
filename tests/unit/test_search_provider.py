import pytest
from yente.data.loader import read_path_lines
from yente.search.base import SearchProvider, Index

from unittest.mock import MagicMock, patch


@patch("yente.data.dataset.Dataset")
@pytest.mark.asyncio
async def test_index_creation(MockDataset):
    m = MockDataset()
    m.name = "test"
    m.next_version = MagicMock(return_value="5")
    provider = await SearchProvider.create()
    index = Index(provider, m)
    assert index.index_name == "yente-entities-test"
    try:
        assert await index.exists() is False
        await index.upsert()
        assert await index.exists() is True
        # Creating the same index again should not raise an error
        try:
            await index.upsert()
        except Exception as e:
            raise AssertionError(
                f"It should be possible to create the same index twice: {e}"
            )
    finally:
        await index.delete()
        await provider.client.close()


@patch("yente.data.dataset.Dataset")
@pytest.mark.asyncio
async def test_index_cloning(MockDataset):
    m = MockDataset()
    m.name = "test"
    m.next_version = MagicMock(return_value="5")
    provider = await SearchProvider.create()
    index = Index(provider, m)
    try:
        await index.upsert()
        clone = await index.clone()
        assert await clone.exists() is True
    finally:
        await index.delete()
        await clone.delete()
        await provider.client.close()


@patch("yente.data.dataset.Dataset")
@pytest.mark.asyncio
async def test_bulk_updating(MockDataset, fake_deltas_path):
    m = MockDataset()
    m.name = "test"
    m.next_version = MagicMock(return_value="5")
    provider = await SearchProvider.create()
    index = Index(provider, m)
    try:
        await index.upsert()
        await index.bulk_update(read_path_lines(fake_deltas_path))
    except Exception as e:
        print(e)
    finally:
        await index.delete()
        await provider.client.close()
