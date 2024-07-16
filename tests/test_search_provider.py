# mypy: ignore-errors
import pytest
from yente import settings

from yente.exc import YenteIndexError, YenteNotFoundError
from yente.provider import SearchProvider


@pytest.mark.asyncio
async def test_provider_core(search_provider: SearchProvider):
    # Not sure what to test....
    with pytest.raises(YenteNotFoundError):
        fake_index = settings.ENTITY_INDEX + "-doesnt-exist"
        await search_provider.refresh(fake_index)
        await search_provider.check_health(fake_index)

    temp_index = settings.ENTITY_INDEX + "-provider-admin"
    await search_provider.create_index(temp_index)
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
    await search_provider.create_index(temp_index)
    post_indices = await search_provider.get_all_indices()
    assert temp_index in post_indices
    assert len(post_indices) == len(pre_indices) + 1
    # If it already exists we expect no error
    await search_provider.create_index(temp_index)

    with pytest.raises(YenteIndexError):
        await search_provider.create_index(temp_index + "_FAIL")

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
    await search_provider.create_index(index_v1)
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
