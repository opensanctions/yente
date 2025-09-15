import pytest
from yente.search.audit_log import (
    get_audit_log_index_name,
    AuditLogEventType,
    log_audit_message,
)
from yente.provider import SearchProvider

TEST_INDEX_NAME = "test-index"
TEST_DATASET = "test-dataset"
TEST_DATASET_VERSION = "20250813-abc"


@pytest.mark.asyncio
async def test_audit_log(search_provider: SearchProvider):
    await search_provider.delete_index(get_audit_log_index_name())

    await log_audit_message(
        search_provider,
        index=TEST_INDEX_NAME,
        dataset=TEST_DATASET,
        dataset_version=TEST_DATASET_VERSION,
        event_type=AuditLogEventType.REINDEX_STARTED,
        message=f"Full reindex of {TEST_DATASET} to {TEST_INDEX_NAME} started",
    )

    await log_audit_message(
        search_provider,
        index=TEST_INDEX_NAME,
        event_type=AuditLogEventType.CLEANUP_INDEX_DELETED,
        message="Cleanup of old indices with prefix yente-entities started",
    )

    await search_provider.refresh(get_audit_log_index_name())

    oldest_result = await search_provider.search(
        get_audit_log_index_name(),
        {"match_all": {}},
        size=1,
        sort=[{"timestamp": {"order": "asc"}}],
    )
    oldest_hit = oldest_result["hits"]["hits"][0]["_source"]
    assert oldest_hit["index"] == TEST_INDEX_NAME
    assert oldest_hit["event_type"] == AuditLogEventType.REINDEX_STARTED

    newest_result = await search_provider.search(
        get_audit_log_index_name(),
        {"match_all": {}},
        size=1,
        sort=[{"timestamp": {"order": "desc"}}],
    )
    newest_hit = newest_result["hits"]["hits"][0]["_source"]
    assert newest_hit["event_type"] == AuditLogEventType.CLEANUP_INDEX_DELETED
    assert newest_hit["index"] == TEST_INDEX_NAME
