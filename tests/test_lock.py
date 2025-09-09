import pytest
from unittest.mock import patch
from datetime import datetime, timedelta

from yente.search.lock import (
    acquire_lock,
    release_lock,
    refresh_lock,
    get_lock_index_name,
)
from yente.provider import SearchProvider

TEST_INDEX_NAME = "test-index"
TEST_DATASET = "test-dataset"
TEST_DATASET_VERSION = "test-dataset-version"


@pytest.mark.asyncio
async def test_lock(search_provider: SearchProvider):
    """Test the happy case: acquire, refresh, release"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(get_lock_index_name())

    lock_session = await acquire_lock(search_provider)
    assert lock_session is not None

    # Try to acquire the same lock again (should fail)
    lock_session_again = await acquire_lock(search_provider)
    assert lock_session_again is None

    # Refresh the lock
    refreshed = await refresh_lock(search_provider, lock_session)
    assert refreshed is True

    # Release the lock
    await release_lock(search_provider, lock_session)

    # Should be able to acquire the lock again after release
    acquired_after_release = await acquire_lock(search_provider)
    assert acquired_after_release is not None

    # Clean up
    await release_lock(search_provider, acquired_after_release)


@pytest.mark.asyncio
async def test_lock_not_expired(search_provider: SearchProvider):
    """Test that acquiring the lock again after one minute will fail, but after 10 minutes will work"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(get_lock_index_name())

    # Acquire initial lock
    lock_session = await acquire_lock(search_provider)
    assert lock_session is not None

    # Get the original lock time for reference
    original_time = datetime.now()

    # Mock time to simulate 1 minute has passed
    with patch("yente.search.lock.datetime") as mock_datetime:
        # Set up the mock to return a time 1 minute after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=1)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 1 minute (should fail)
        acquired_after_1min = await acquire_lock(search_provider)
        assert acquired_after_1min is None

    # Mock time to simulate 10 minutes have passed (beyond the 5-minute expiration)
    with patch("yente.search.lock.datetime") as mock_datetime:
        # Set up the mock to return a time 10 minutes after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=10)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 10 minutes (should succeed)
        acquired_after_10min = await acquire_lock(search_provider)
        assert acquired_after_10min is not None

    # Clean up
    await release_lock(search_provider, lock_session)


@pytest.mark.asyncio
async def test_refresh_expired_lock(search_provider: SearchProvider):
    """Test that refreshing an expired lock will fail"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(get_lock_index_name())

    original_time = datetime.now()

    # Acquire initial lock
    lock_session = await acquire_lock(search_provider)
    assert lock_session is not None

    # Mock time to simulate 1 minute has passed
    with patch("yente.search.lock.datetime") as mock_datetime:
        # Set up the mock to return a time 1 minute after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=20)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to refresh the lock after 20 minutes (should fail)
        refreshed = await refresh_lock(search_provider, lock_session)
        assert refreshed is False

    # Clean up
    await release_lock(search_provider, lock_session)
