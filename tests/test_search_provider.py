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


def _make_resource_already_exists_error(provider: SearchProvider) -> Exception:
    """Return a resource_already_exists_exception error for the given provider."""
    if isinstance(provider, OpenSearchProvider):
        from opensearchpy.exceptions import RequestError

        return RequestError(400, "resource_already_exists_exception", {})
    from elastic_transport import ApiResponseMeta, HttpHeaders
    from elasticsearch import ApiError

    meta = ApiResponseMeta(
        status=400,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=None,
    )
    return ApiError("resource_already_exists_exception", meta=meta, body={})


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
    await search_provider.clone_index(index_v1, index_v2)

    with pytest.raises(YenteIndexError):
        await search_provider.clone_index(index_fail, index_v2)

    assert not await search_provider.exists_index_alias(alias, index_v1)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == []

    with pytest.raises(YenteIndexError):
        await search_provider.rollover_index(alias, index_fail, prefix=prefix)
    await search_provider.rollover_index(alias, index_v1, prefix=prefix)
    assert await search_provider.exists_index_alias(alias, index_v1)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == [index_v1]

    await search_provider.rollover_index(alias, index_v2, prefix=prefix)
    assert not await search_provider.exists_index_alias(alias, index_v1)
    assert await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == [index_v2]

    await search_provider.delete_index(index_v2)
    assert not await search_provider.exists_index_alias(alias, index_v2)
    assert await search_provider.get_alias_indices(alias) == []


@pytest.mark.asyncio
async def test_opensearch_connection_timeout(search_provider: SearchProvider):
    """The OpenSearch client connection-level timeout must be >= 60s.

    opensearchpy does NOT propagate `request_timeout` to the connection layer
    (which defaults to 10s). Without an explicit `timeout` kwarg, long operations
    like clone_index time out at the HTTP level, causing spurious retries and
    `resource_already_exists_exception` errors.
    """
    if not isinstance(search_provider, OpenSearchProvider):
        pytest.skip("Only applies to OpenSearchProvider")

    pool = search_provider.client.transport.connection_pool
    for conn in pool.connections:
        assert conn.timeout >= 60, (
            f"Connection timeout is {conn.timeout}s, expected >= 60s. "
            "The `timeout` kwarg must be passed to AsyncOpenSearch() so it "
            "propagates to AsyncHttpConnection."
        )


@pytest.mark.asyncio
async def test_clone_index_recovers_from_timeout(search_provider: SearchProvider):
    """End-to-end: simulate clone timeout with real health check on real index.

    1. Create source, clone to target (real clone, simulates server-side success)
    2. Mock indices.clone to raise resource_already_exists_exception
    3. Verify clone_index recovers (check_health hits real healthy target)
    """
    if not isinstance(search_provider, OpenSearchProvider):
        pytest.skip(
            "Only applies to OpenSearchProvider (Elastic check_health uses timeout=0)"
        )

    source = settings.ENTITY_INDEX + "-clone-recov-src"
    target = settings.ENTITY_INDEX + "-clone-recov-tgt"

    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    # Real clone — simulates a server-side clone that completed after client timeout
    await search_provider.clone_index(source, target)
    assert target in await search_provider.get_all_indices()

    try:
        indices_client = _get_indices_client(search_provider)
        error = _make_resource_already_exists_error(search_provider)

        # Mock clone to raise resource_already_exists (what the transport retry
        # sees after the first attempt timed out but succeeded server-side).
        # Also mock delete_index so it doesn't remove the real target.
        with patch.object(
            type(indices_client), "clone", new_callable=AsyncMock, side_effect=error
        ):
            with patch.object(search_provider, "delete_index", new_callable=AsyncMock):
                # Should NOT raise — real health check detects healthy target
                await search_provider.clone_index(source, target)

        # Target should still exist and be healthy
        assert target in await search_provider.get_all_indices()
        assert await search_provider.check_health(target) is True
    finally:
        await search_provider.delete_index(source)
        await search_provider.delete_index(target)


@pytest.mark.asyncio
async def test_clone_index_already_exists_healthy(search_provider: SearchProvider):
    """When clone raises resource_already_exists_exception and the target is
    healthy, clone_index should treat it as success (idempotent clone)."""
    source = settings.ENTITY_INDEX + "-clone-idem-src"
    target = settings.ENTITY_INDEX + "-clone-idem-tgt"

    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    try:
        indices_client = _get_indices_client(search_provider)
        error = _make_resource_already_exists_error(search_provider)

        with patch.object(
            type(indices_client), "clone", new_callable=AsyncMock, side_effect=error
        ):
            # Mock check_health to return True (healthy target)
            with patch.object(
                search_provider,
                "check_health",
                new_callable=AsyncMock,
                return_value=True,
            ):
                # Should NOT raise — the target is reported as healthy
                await search_provider.clone_index(source, target)
    finally:
        await search_provider.delete_index(source)
        await search_provider.delete_index(target)


@pytest.mark.asyncio
async def test_clone_index_already_exists_unhealthy(search_provider: SearchProvider):
    """When clone raises resource_already_exists_exception but the target is
    unhealthy, clone_index should clean up and re-raise."""
    source = settings.ENTITY_INDEX + "-clone-unhl-src"
    target = settings.ENTITY_INDEX + "-clone-unhl-tgt"

    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    # Pre-create the target so delete_index inside clone_index can find it
    await search_provider.create_index(
        target, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    try:
        indices_client = _get_indices_client(search_provider)
        error = _make_resource_already_exists_error(search_provider)

        with patch.object(
            type(indices_client), "clone", new_callable=AsyncMock, side_effect=error
        ):
            # Mock check_health to return False (unhealthy target)
            with patch.object(
                search_provider,
                "check_health",
                new_callable=AsyncMock,
                return_value=False,
            ):
                with pytest.raises(YenteIndexError):
                    await search_provider.clone_index(source, target)

        # Target should have been cleaned up
        assert target not in await search_provider.get_all_indices()
    finally:
        await search_provider.delete_index(source)
        await search_provider.delete_index(target)


@pytest.mark.asyncio
async def test_clone_index_failure_restores_read_only(search_provider: SearchProvider):
    """Regression test: clone_index must restore read_only=False on the source
    index even when the clone operation fails (#1033)."""
    source = settings.ENTITY_INDEX + "-clone-ro-src"
    target = settings.ENTITY_INDEX + "-clone-ro-tgt"

    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    try:
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
    finally:
        # Ensure source is writable for cleanup (in case fix is not yet applied)
        try:
            if isinstance(search_provider, OpenSearchProvider):
                await search_provider.client.indices.put_settings(
                    index=source,
                    body={"settings": {"index.blocks.read_only": False}},
                )
            elif isinstance(search_provider, ElasticSearchProvider):
                await search_provider.client().indices.put_settings(
                    index=source,
                    settings={"index.blocks.read_only": False},
                )
        except Exception:
            pass
        await search_provider.delete_index(source)
        await search_provider.delete_index(target)
