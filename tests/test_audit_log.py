import pytest
from yente.search.audit_log import (
    get_audit_log_index_name,
    AuditLogMessageType,
    log_audit_message,
)
from yente.provider import SearchProvider


@pytest.mark.asyncio
async def test_audit_log(search_provider: SearchProvider):
    await search_provider.delete_index(get_audit_log_index_name())

    await log_audit_message(
        search_provider, "test-index-1", AuditLogMessageType.FULL_REINDEX_STARTED
    )
    await log_audit_message(
        search_provider, "test-index-2", AuditLogMessageType.PARTIAL_REINDEX_COMPLETED
    )
    await log_audit_message(
        search_provider, "test-index-1", AuditLogMessageType.FULL_REINDEX_FAILED
    )
    await log_audit_message(
        search_provider,
        "test-index-4",
        AuditLogMessageType.INDEX_ALIAS_ROLLOVER_COMPLETE,
    )

    await search_provider.refresh(get_audit_log_index_name())

    oldest_result = await search_provider.search(
        get_audit_log_index_name(),
        {"match_all": {}},
        size=1,
        sort=[{"timestamp": {"order": "asc"}}],
    )
    oldest_hit = oldest_result["hits"]["hits"][0]["_source"]
    assert oldest_hit["index"] == "test-index-1"
    assert oldest_hit["message_type"] == AuditLogMessageType.FULL_REINDEX_STARTED

    newest_result = await search_provider.search(
        get_audit_log_index_name(),
        {"match_all": {}},
        size=1,
        sort=[{"timestamp": {"order": "desc"}}],
    )
    newest_hit = newest_result["hits"]["hits"][0]["_source"]
    assert newest_hit["index"] == "test-index-4"
    assert (
        newest_hit["message_type"] == AuditLogMessageType.INDEX_ALIAS_ROLLOVER_COMPLETE
    )
