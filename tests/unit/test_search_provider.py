import pytest
from yente.data.loader import read_path_lines
from yente.search.base import ESSearchProvider, Index
from yente import settings
from unittest.mock import MagicMock, patch
from typing import AsyncGenerator, List


async def as_generator(lst: List) -> AsyncGenerator:
    for i in lst:
        yield i


operations = [
    {
        "op": "ADD",
        "entity": {
            "id": "1",
            "name": "test",
            "schema": "Person",
            "properties": {},
        },
    },
    {
        "op": "MOD",
        "entity": {"id": "1", "name": "Vorgle", "schema": "Person", "properties": {}},
    },
    {
        "op": "DEL",
        "entity": {"id": "1"},
    },
]


class TestSearchProvider:
    @pytest.mark.asyncio
    async def test_upsert_index(self, search_provider):
        # Given a non-existent index
        # When creating it we should return nothing
        res = await search_provider.upsert_index("test")
        assert res is None
        assert await search_provider.index_exists("test") is True
        # If it already exists we expect no error
        res = await search_provider.upsert_index("test")
        assert res is None

    @pytest.mark.asyncio
    async def test_delete_index(self, search_provider):
        # Given an existing index
        await search_provider.upsert_index("test")
        # It should be possible to delete it
        await search_provider.delete_index("test")
        assert await search_provider.index_exists("test") is False
        # If it does not exist we do nothing
        assert await search_provider.index_exists("does_not_exist") is False
        try:
            await search_provider.delete_index("does_not_exist")
        except Exception as e:
            raise AssertionError(
                f"Deleting a non-existent index should not raise an error: {e}"
            )

    @pytest.mark.asyncio
    async def test_alias(self, search_provider):
        # Given an index
        await search_provider.upsert_index("test_1")
        # It should be possible to create an alias for it
        await search_provider.add_alias("test_1", "test_alias")
        # If the alias already has the index as a source, do nothing
        try:
            await search_provider.add_alias("test_1", "test_alias")
        except Exception as e:
            raise AssertionError(
                f"Adding an index to an alias twice should not raise an error: {e}"
            )
        # If the index does not exist, raise an error
        with pytest.raises(Exception):
            await search_provider.add_alias("does_not_exist", "test_alias")
        # It should be possible to get the sources for an index
        await search_provider.upsert_index("test_2")
        await search_provider.add_alias("test_2", "test_alias")
        sources = await search_provider.get_alias_sources("test_alias")
        assert sorted(sources) == ["test_1", "test_2"]
        # It should be possible to switch the sources for an alias based off of a prefix
        await search_provider.alias_rollover("test_alias", "test_2", "test_")
        sources = await search_provider.get_alias_sources("test_alias")
        assert sorted(sources) == ["test_2"]

    @pytest.mark.asyncio
    async def test_cloning(self, search_provider):
        # Given an existing index
        await search_provider.upsert_index("test")
        # It should be possible to clone it
        await search_provider.clone_index("test", "test_clone")
        assert await search_provider.index_exists("test_clone") is True
        # If the source does not exist, raise an error
        with pytest.raises(Exception):
            await search_provider.clone_index("does_not_exist", "test_clone")
        # If the target already exists, raise an error
        with pytest.raises(Exception):
            await search_provider.clone_index("test", "test_clone")
        # If the source and target are the same, raise an error
        with pytest.raises(Exception):
            await search_provider.clone_index("test", "test")

    @pytest.mark.asyncio
    async def test_updating(self, search_provider):
        # It should be able to parse all operation types
        for op in operations:
            try:
                search_provider._to_operation(op, "test")
            except Exception as e:
                raise AssertionError(f"Operation {op['op']} should be parsable: {e}")
        # Given an existing index
        await search_provider.upsert_index("test")
        # It should be possible to update it with a list of documents
        docs = [
            {
                "op": "ADD",
                "entity": {
                    "id": "1",
                    "name": "test",
                    "schema": "Person",
                    "properties": {},
                },
            },
            {
                "op": "ADD",
                "entity": {
                    "id": "2",
                    "name": "Claude Foobar Michel",
                    "schema": "Person",
                    "properties": {},
                },
            },
        ]
        await search_provider.update(as_generator(docs), "test")
        await search_provider.refresh("test")
        assert await search_provider.count("test") == 2
        # It should be possible to delete a document
        await search_provider.update(
            as_generator([{"op": "DEL", "entity": {"id": "2"}}]), "test"
        )
        await search_provider.refresh("test")
        assert await search_provider.count("test") == 1
        # Modify the document does not change the index count
        await search_provider.update(
            as_generator(
                [
                    {
                        "op": "MOD",
                        "entity": {
                            "id": "1",
                            "name": "Theresa May",
                            "schema": "Person",
                            "properties": {"hobbies": "Running through wheat fields."},
                        },
                    }
                ]
            ),
            "test",
        )
        await search_provider.refresh("test")
        assert await search_provider.count("test") == 1


class TestIndex:
    @pytest.mark.asyncio
    async def test_switchover(self, search_provider):
        # It is possible to make an index the main index
        index = Index(search_provider, "test", "1")
        await index.upsert()
        await index.make_main()
        indices = await search_provider.get_backing_indexes(settings.ENTITY_INDEX)
        assert index.name in indices
        # It is possible to switch over to a new index
        new_index = Index(search_provider, "test", "2")
        await new_index.upsert()
        await new_index.make_main()
        indices = await search_provider.get_backing_indexes(settings.ENTITY_INDEX)
        assert new_index.name in indices
        assert index.name not in indices


@patch("yente.data.manifest.Dataset")
@pytest.mark.asyncio
async def test_index_creation(MockDataset):
    m = MockDataset()
    m.name = "test"
    m.version = "4"
    m.next_version = MagicMock(return_value="5")
    provider = await ESSearchProvider.create()
    index = Index(provider, m.name, m.version)
    assert (
        index.name
        == f"{settings.ENTITY_INDEX}-{m.name}-{settings.INDEX_VERSION}{m.version}"
    )
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
    provider = await ESSearchProvider.create()
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
    m.version = "4"
    m.next_version = MagicMock(return_value="5")
    provider = await ESSearchProvider.create()
    index = Index(provider, m.name, m.version)
    try:
        await index.upsert()
        await index.bulk_update(read_path_lines(fake_deltas_path))
    except Exception as e:
        print(e)
    finally:
        await index.delete()
        await provider.client.close()
