import pytest
from unittest.mock import patch
from datetime import datetime, timedelta

from yente.search.audit_log import (
    AuditLogReindexType,
    get_audit_log_index_name,
    acquire_reindex_lock,
    release_reindex_lock,
    refresh_reindex_lock,
)
from yente.provider import SearchProvider

TEST_INDEX_NAME = "test-index"
TEST_DATASET = "test-dataset"
TEST_DATASET_VERSION = "test-dataset-version"
TEST_REINDEX_TYPE = AuditLogReindexType.FULL


@pytest.mark.asyncio
async def test_lock(search_provider: SearchProvider):
    """Test the happy case: acquire, refresh, release"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(get_audit_log_index_name())

    acquired = await acquire_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )
    assert acquired is True

    # Try to acquire the same lock again (should fail)
    acquired_again = await acquire_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )
    assert acquired_again is False

    # Refresh the lock
    refreshed = await refresh_reindex_lock(search_provider, TEST_INDEX_NAME)
    assert refreshed is True

    # Release the lock
    await release_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )

    # Should be able to acquire the lock again after release
    acquired_after_release = await acquire_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )
    assert acquired_after_release is True

    # Clean up
    await release_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )


@pytest.mark.asyncio
async def test_lock_not_expired(search_provider: SearchProvider):
    """Test that acquiring the lock again after one minute will fail, but after 10 minutes will work"""
    # Clean slate: delete any old lock index
    await search_provider.delete_index(get_audit_log_index_name())

    # Acquire initial lock
    acquired = await acquire_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )
    assert acquired is True

    # Get the original lock time for reference
    original_time = datetime.now()

    # Mock time to simulate 1 minute has passed
    with patch("yente.search.audit_log.datetime") as mock_datetime:
        # Set up the mock to return a time 1 minute after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=1)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 1 minute (should fail)
        acquired_after_1min = await acquire_reindex_lock(
            search_provider,
            TEST_INDEX_NAME,
            dataset=TEST_DATASET,
            dataset_version=TEST_DATASET_VERSION,
            reindex_type=TEST_REINDEX_TYPE,
        )
        assert acquired_after_1min is False

    # Mock time to simulate 10 minutes have passed (beyond the 5-minute expiration)
    with patch("yente.search.audit_log.datetime") as mock_datetime:
        # Set up the mock to return a time 10 minutes after the original time
        mock_datetime.now.return_value = original_time + timedelta(minutes=10)
        # Keep the fromtimestamp method working for parsing timestamps
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        # Try to acquire the lock again after 10 minutes (should succeed)
        acquired_after_10min = await acquire_reindex_lock(
            search_provider,
            TEST_INDEX_NAME,
            dataset=TEST_DATASET,
            dataset_version=TEST_DATASET_VERSION,
            reindex_type=TEST_REINDEX_TYPE,
        )
        assert acquired_after_10min is True

    # Clean up
    await release_reindex_lock(
        search_provider,
        TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        reindex_type=TEST_REINDEX_TYPE,
    )
