# mypy: ignore-errors
from unittest.mock import AsyncMock, patch

import pytest

from yente import settings
from yente.exc import YenteIndexError, YenteNotFoundError
from yente.provider import SearchProvider
from yente.provider.elastic import ElasticSearchProvider
from yente.provider.opensearch import OpenSearchProvider
from yente.search.mapping import INDEX_SETTINGS, make_entity_mapping

# Constants for testing
TEST_MAPPINGS = make_entity_mapping()
TEST_SETTINGS = INDEX_SETTINGS


def _get_indices_client(provider: SearchProvider):
    """Return the raw indices client for the given provider."""
    if isinstance(provider, OpenSearchProvider):
        return provider.client.indices
    elif isinstance(provider, ElasticSearchProvider):
        return provider.client().indices
    raise TypeError(f"Unsupported provider type: {type(provider)}")


def _make_transport_error(provider: SearchProvider) -> Exception:
    """Return a TransportError appropriate for the given provider."""
    if isinstance(provider, OpenSearchProvider):
        from opensearchpy.exceptions import TransportError

        return TransportError(500, "simulated clone failure", {})
    from elasticsearch import TransportError

    return TransportError("simulated clone failure")


@pytest.mark.asyncio
async def test_provider_core(search_provider: SearchProvider):
    # Not sure what to test....
    with pytest.raises(YenteNotFoundError):
        fake_index = settings.ENTITY_INDEX + "-doesnt-exist"
        await search_provider.refresh(fake_index)
        await search_provider.check_health(fake_index)
        await search_provider.search(fake_index, {})

    temp_index = settings.ENTITY_INDEX + "-provider-admin"
    await search_provider.create_index(
        temp_index, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    await search_provider.refresh(temp_index)
    assert await search_provider.check_health(temp_index) is True
    await search_provider.delete_index(temp_index)


@pytest.mark.asyncio
async def test_index_lifecycle(search_provider: SearchProvider):
    # Given a non-existent index
    # When creating it we should return nothing
    temp_index = settings.ENTITY_INDEX + "-provider-test"
    pre_indices = await search_provider.get_all_indices()
    assert temp_index not in pre_indices
    await search_provider.create_index(
        temp_index, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    post_indices = await search_provider.get_all_indices()
    assert temp_index in post_indices
    assert len(post_indices) == len(pre_indices) + 1
    # If it already exists we expect no error
    await search_provider.create_index(
        temp_index, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )

    with pytest.raises(YenteIndexError):
        await search_provider.create_index(
            temp_index + "_FAIL", mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
        )

    await search_provider.refresh(temp_index)

    await search_provider.delete_index(temp_index)
    del_indices = await search_provider.get_all_indices()
    assert temp_index not in del_indices
    assert len(del_indices) == len(pre_indices)


@pytest.mark.asyncio
async def test_alias_management(search_provider: SearchProvider):
    alias = settings.ENTITY_INDEX + "-alias"
    prefix = alias + "-prefix"
    index_v1 = prefix + "-v1"
    index_v2 = prefix + "-v2"
    index_fail = prefix + "-fail"
    await search_provider.create_index(
        index_v1, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    # Cloning a non-existent index raises YenteIndexError
    await search_provider.clone_index(index_v1, index_v2)
    with pytest.raises(YenteIndexError):
        await search_provider.clone_index(index_fail, index_v2)

    # Before any rollover, neither index is aliased.
    assert not await search_provider.exists_index_alias(alias, index_v1)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == []

    # Rolling over to a non-existent index raises YenteIndexError.
    with pytest.raises(YenteIndexError):
        await search_provider.rollover_index(alias, index_fail, prefix=prefix)
    # Rolling over to v1 points the alias at v1 only.
    await search_provider.rollover_index(alias, index_v1, prefix=prefix)
    assert await search_provider.exists_index_alias(alias, index_v1)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == [index_v1]

    # Rolling over to v2 atomically swaps the alias: v1 is removed, v2 is added.
    await search_provider.rollover_index(alias, index_v2, prefix=prefix)
    assert not await search_provider.exists_index_alias(alias, index_v1)
    assert await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == [index_v2]

    # Deleting the backing index removes it from the alias automatically.
    await search_provider.delete_index(index_v2)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == []


@pytest.mark.asyncio
async def test_clone_index_failure_restores_read_only(search_provider: SearchProvider):
    """Regression test: clone_index must restore read_only=False on the source
    index even when the clone operation fails (#1033)."""
    source = settings.ENTITY_INDEX + "-clone-ro-src"
    target = settings.ENTITY_INDEX + "-clone-ro-tgt"

    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    indices_client = _get_indices_client(search_provider)
    error = _make_transport_error(search_provider)

    # Patch clone at the class level so it affects all instances
    # (important for ElasticSearchProvider where client() creates new objects)
    with patch.object(
        type(indices_client), "clone", new_callable=AsyncMock, side_effect=error
    ):
        with pytest.raises(YenteIndexError):
            await search_provider.clone_index(source, target)

    # Verify the source index is NOT read-only
    resp = await indices_client.get_settings(index=source)
    index_settings = resp[source]["settings"]["index"]
    blocks = index_settings.get("blocks", {})
    read_only = blocks.get("read_only", "false")
    assert (
        str(read_only).lower() != "true"
    ), f"Source index is still read-only after failed clone: {read_only}"

    # Clean up
    await search_provider.delete_index(source)
    await search_provider.delete_index(target)
