# mypy: ignore-errors
import pytest
from unittest.mock import patch
from datetime import datetime, timedelta

from yente.search.lock import (
    LOCK_INDEX,
    acquire_lock,
    release_lock,
    refresh_lock,
)
from yente.provider import SearchProvider

TEST_INDEX_NAME = "test-index"


@pytest.mark.asyncio
async def test_lock(search_provider: SearchProvider):
    """Test the happy case: acquire, refresh, release"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(LOCK_INDEX)

    acquired = await acquire_lock(search_provider, TEST_INDEX_NAME)
    assert acquired is True

    # Try to acquire the same lock again (should fail)
    acquired_again = await acquire_lock(search_provider, TEST_INDEX_NAME)
    assert acquired_again is False

    # Refresh the lock
    refreshed = await refresh_lock(search_provider, TEST_INDEX_NAME)
    assert refreshed is True

    # Release the lock
    await release_lock(search_provider, TEST_INDEX_NAME)

    # Should be able to acquire the lock again after release
    acquired_after_release = await acquire_lock(search_provider, TEST_INDEX_NAME)
    assert acquired_after_release is True

    # Clean up
    await release_lock(search_provider, TEST_INDEX_NAME)


@pytest.mark.asyncio
async def test_lock_not_expired(search_provider: SearchProvider):
    """Test that acquiring the lock again after one minute will fail, but after 10 minutes will work"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(LOCK_INDEX)

    # Acquire initial lock
    acquired = await acquire_lock(search_provider, TEST_INDEX_NAME)
    assert acquired is True

    # Get the original lock time for reference
    original_time = datetime.now()

    # Mock time to simulate 1 minute has passed
    with patch("yente.search.lock.datetime") as mock_datetime:
        # Set up the mock to return a time 1 minute after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=1)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 1 minute (should fail)
        acquired_after_1min = await acquire_lock(search_provider, TEST_INDEX_NAME)
        assert acquired_after_1min is False

    # Mock time to simulate 10 minutes have passed (beyond the 5-minute expiration)
    with patch("yente.search.lock.datetime") as mock_datetime:
        # Set up the mock to return a time 10 minutes after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=10)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 10 minutes (should succeed)
        acquired_after_10min = await acquire_lock(search_provider, TEST_INDEX_NAME)
        assert acquired_after_10min is True

    # Clean up
    await release_lock(search_provider, TEST_INDEX_NAME)
