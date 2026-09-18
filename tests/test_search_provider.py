# mypy: ignore-errors
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders
from elasticsearch import ApiError, NotFoundError
from opensearchpy.exceptions import NotFoundError as OpenSearchNotFoundError
from opensearchpy.exceptions import TransportError as OpenSearchTransportError

from yente import settings
from yente.exc import IndexNotReadyError, YenteIndexError, YenteNotFoundError
from yente.provider import SearchProvider
from yente.provider.elastic import ElasticSearchProvider
from yente.provider.opensearch import OpenSearchProvider, OpenSearchServiceType
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
    with pytest.raises(YenteIndexError):
        await search_provider.clone_index(index_fail, index_v2)

    # Clone index_v1 to index_v2 as setup for the test
    await search_provider.clone_index(index_v1, index_v2)

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
    with (
        patch.object(
            type(indices_client), "clone", new_callable=AsyncMock, side_effect=error
        ),
        pytest.raises(YenteIndexError),
    ):
        await search_provider.clone_index(source, target)

    # Verify the source index is NOT read-only
    resp = await indices_client.get_settings(index=source)
    index_settings = resp[source]["settings"]["index"]
    blocks = index_settings.get("blocks", {})
    read_only = blocks.get("read_only", "false")
    assert str(read_only).lower() != "true", (
        f"Source index is still read-only after failed clone: {read_only}"
    )

    # Clean up
    await search_provider.delete_index(source)
    await search_provider.delete_index(target)


@pytest.mark.asyncio
async def test_index_metadata_roundtrip(search_provider: SearchProvider):
    temp_index = settings.ENTITY_INDEX + "-meta-test"
    await search_provider.create_index(
        temp_index, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    try:
        # Fresh index has no metadata
        assert await search_provider.get_index_metadata(temp_index) == {}

        # Round-trip a multi-key metadata dict
        meta_v1 = {"last_export": "2026-06-01T12:00:00", "other": "x"}
        await search_provider.set_index_metadata(temp_index, meta_v1)
        assert await search_provider.get_index_metadata(temp_index) == meta_v1

        # Replace-not-merge: setting a smaller dict drops keys not in it
        meta_v2 = {"last_export": "2026-06-02T12:00:00"}
        await search_provider.set_index_metadata(temp_index, meta_v2)
        assert await search_provider.get_index_metadata(temp_index) == meta_v2

        # Clearing
        await search_provider.set_index_metadata(temp_index, {})
        assert await search_provider.get_index_metadata(temp_index) == {}
    finally:
        await search_provider.delete_index(temp_index)


@pytest.mark.asyncio
async def test_index_metadata_clone_inherits_then_overwrites(
    search_provider: SearchProvider,
):
    """Cloning copies the source's _meta to the target. Our indexer relies on
    set_index_metadata being able to overwrite that inherited value."""
    source = settings.ENTITY_INDEX + "-meta-clone-src"
    target = settings.ENTITY_INDEX + "-meta-clone-tgt"
    await search_provider.create_index(
        source, mappings=TEST_MAPPINGS, settings=TEST_SETTINGS
    )
    try:
        await search_provider.set_index_metadata(source, {"last_export": "A"})
        await search_provider.clone_index(source, target)
        try:
            # Pin the ES behavior we're relying on
            assert await search_provider.get_index_metadata(target) == {
                "last_export": "A"
            }
            # And our overwrite works on the cloned index
            await search_provider.set_index_metadata(target, {"last_export": "B"})
            assert await search_provider.get_index_metadata(target) == {
                "last_export": "B"
            }
        finally:
            await search_provider.delete_index(target)
    finally:
        await search_provider.delete_index(source)


@pytest.mark.asyncio
@pytest.mark.usefixtures("zala_test_dataset")
async def test_search_track_total_hits(search_provider: SearchProvider):
    query = {"match_all": {}}
    counted = await search_provider.search(settings.ENTITY_INDEX, query, size=1)
    assert counted["hits"]["total"]["value"] > 1
    assert len(counted["hits"]["hits"]) == 1

    uncounted = await search_provider.search(
        settings.ENTITY_INDEX, query, size=1, track_total_hits=False
    )
    assert "total" not in uncounted["hits"]
    assert len(uncounted["hits"]["hits"]) == 1


def elastic_search_phase_error(status: int) -> ApiError:
    """The error Elasticsearch answers a search it could not complete with.

    The index raises the same error whether it could not run the query at all or
    could not run it on every shard, and only the status tells the two apart.
    ApiResponseMeta is how the elasticsearch library carries that status.
    """
    meta = ApiResponseMeta(
        status=status,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=None,
    )
    return ApiError(message="search_phase_execution_exception", meta=meta, body={})


def opensearch_search_phase_error(status):
    """The equivalent error from the OpenSearch client.

    Its TransportError carries the status as the first argument, which is the
    string 'N/A' for a failure that never reached the index.
    """
    return OpenSearchTransportError(status, "search_phase_execution_exception", {})


def elastic_provider_searching_with(search: AsyncMock) -> ElasticSearchProvider:
    """A provider whose index answers a search with the given mock.

    The provider reaches its client through client(), which returns
    self._client.options(...) so that each request carries its trace headers.
    The stand-in client therefore answers .options() with the object holding the
    search, which is what the provider ends up calling.
    """
    client = MagicMock()
    client.options.return_value = MagicMock(search=search)
    return ElasticSearchProvider(client)


def opensearch_provider_searching_with(search: AsyncMock) -> OpenSearchProvider:
    """A provider whose index answers a search with the given mock."""
    return OpenSearchProvider(
        MagicMock(search=search), service_type=OpenSearchServiceType.ES
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 404, 429, 500, 503])
async def test_elastic_search_carries_through_an_index_error(status: int):
    """A search the index could not run on every shard is left to be retried.

    The index raises the same error as it does for a query it cannot run, so what
    is read here is that the status it gave is the status the caller sees.
    Reported as the 400 this once was, the caller reads a lost shard or a full
    queue as a bad query and gives up on it.
    """
    search = AsyncMock(side_effect=elastic_search_phase_error(status))

    with pytest.raises(YenteIndexError) as raised:
        await elastic_provider_searching_with(search).search(
            index="idx", query={"match_all": {}}
        )

    assert raised.value.status == status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "given,expected",
    [(400, 400), (404, 404), (429, 429), (503, 503), ("N/A", 500)],
)
async def test_opensearch_search_carries_through_an_index_error(given, expected):
    """The OpenSearch client reports a status of 'N/A' when it never reached the index."""
    search = AsyncMock(side_effect=opensearch_search_phase_error(given))

    with pytest.raises(YenteIndexError) as raised:
        await opensearch_provider_searching_with(search).search(
            index="idx", query={"match_all": {}}
        )

    assert raised.value.status == expected


@pytest.mark.asyncio
async def test_elastic_check_health_reports_a_missing_index_as_not_ready():
    """The readiness check answers 503 for an index the ingestion has not built yet.

    Answered 404, a probe reads a service that is still starting up as one that
    is misconfigured, and /readyz documents 503 for exactly this.
    """
    meta = ApiResponseMeta(
        status=404,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=None,
    )
    health = AsyncMock(
        side_effect=NotFoundError(
            message="index_not_found_exception", meta=meta, body={}
        )
    )
    client = MagicMock()
    client.options.return_value = MagicMock(cluster=MagicMock(health=health))

    with pytest.raises(IndexNotReadyError) as raised:
        await ElasticSearchProvider(client).check_health("idx")

    assert raised.value.status == 503


@pytest.mark.asyncio
async def test_opensearch_check_health_reports_a_missing_index_as_not_ready():
    health = AsyncMock(
        side_effect=OpenSearchNotFoundError(404, "index_not_found_exception", {})
    )
    client = MagicMock(cluster=MagicMock(health=health))
    provider = OpenSearchProvider(client, service_type=OpenSearchServiceType.ES)

    with pytest.raises(IndexNotReadyError) as raised:
        await provider.check_health("idx")

    assert raised.value.status == 503
