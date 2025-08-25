from enum import StrEnum
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, cast
from yente import logs, settings
from yente.provider.base import SearchProvider


# Query the audit log using the following query:
# curl -X GET "localhost:9200/yente-entities-audit-log/_search" -H "Content-Type: application/json" -d '{"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "desc"}}], "size": 10000, "_source": true}' | jq '.hits.hits | map(._source) | reverse'


LOCK_EXPIRATION_TIME = timedelta(minutes=5)

log = logs.get_logger(__name__)


def get_audit_log_index_name() -> str:
    return f"{settings.INDEX_NAME}-audit-log"


def millis_timestamp_to_datetime(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000)


def datetime_to_millis_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class AuditLogReindexType(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


class AuditLogMessageType(StrEnum):
    # REINDEX_LOCK_TENTATIVE gets written first, then we wait for the eventual consistency
    # to settle before then letting the winning reindex write a REINDEX_STARTED message.
    REINDEX_LOCK_TENTATIVE = "reindex_lock_tentative"
    # REINDEX_STARTED gets written as a lock, and is refreshed periodically.
    REINDEX_STARTED = "reindex_started"
    # REINDEX_COMPLETED or REINDEX_FAILED release the lock.
    REINDEX_COMPLETED = "reindex_completed"
    REINDEX_FAILED = "reindex_failed"

    # INDEX_ALIAS_ROLLOVER_COMPLETE is written when the index alias is rolled over
    # and is only for information purposes, it's not used for the locking mechanism.
    INDEX_ALIAS_ROLLOVER_COMPLETE = "index_alias_rollover_complete"


async def ensure_audit_log_index(provider: SearchProvider) -> None:
    if get_audit_log_index_name() in await provider.get_all_indices():
        return

    await provider.create_index(
        get_audit_log_index_name(),
        mappings={
            "properties": {
                "alias_index": {"type": "keyword"},
                "index": {"type": "keyword"},
                "dataset": {"type": "keyword"},
                "dataset_version": {"type": "keyword"},
                "yente_version": {"type": "keyword"},
                "message_type": {"type": "keyword"},
                "reindex_type": {"type": "keyword"},
                "timestamp": {"type": "date", "format": "epoch_millis"},
                # Used to refresh a REINDEX_STARTED lock.
                "heartbeat_timestamp": {"type": "date", "format": "epoch_millis"},
            }
        },
        settings={
            "number_of_shards": 1,
            "auto_expand_replicas": settings.INDEX_AUTO_REPLICAS,
        },
    )


async def log_audit_message(
    provider: SearchProvider,
    message_type: AuditLogMessageType,
    *,
    index: str,
    dataset: str,
    dataset_version: str,
    reindex_type: AuditLogReindexType,
) -> str:
    """Log an audit message to the audit log index and return the document ID."""

    timestamp = datetime_to_millis_timestamp(datetime.now())
    doc_id = f"{index}-{message_type}-{timestamp}"

    await provider.bulk_index(
        [
            {
                "_index": get_audit_log_index_name(),
                "_id": doc_id,
                "_source": {
                    "alias_index": settings.ENTITY_INDEX,
                    "index": index,
                    "dataset": dataset,
                    "dataset_version": dataset_version,
                    "yente_version": settings.VERSION,
                    "message_type": message_type,
                    "reindex_type": reindex_type,
                    "timestamp": timestamp,
                },
            }
        ]
    )
    # Refresh the index to make the write operation visible
    await provider.refresh(get_audit_log_index_name())

    return doc_id


async def _get_most_recent_audit_log_message(
    provider: SearchProvider, index: str
) -> Optional[Dict[str, Any]]:
    """Get the most recent audit log message for a given index.

    Returns a tuple of the document ID and the message source.
    """
    await ensure_audit_log_index(provider)

    result = await provider.search(
        get_audit_log_index_name(),
        {"bool": {"must": [{"term": {"index": index}}]}},
        sort=[{"timestamp": {"order": "desc"}}],
        size=1,
    )
    hits = result.get("hits", {}).get("hits", [])

    if not hits:
        return None

    return cast(Dict[str, Any], hits[0])


def _lock_is_active(most_recent_doc: Optional[Dict[str, Any]]) -> bool:
    """Check if the most recent lock is still valid (not expired).

    Args:
        most_recent_doc: The most recent audit log document or None

    Returns:
        True if the lock is valid and not expired, False otherwise
    """
    if not most_recent_doc:
        return False

    message_type = most_recent_doc["_source"]["message_type"]
    if message_type not in [
        AuditLogMessageType.REINDEX_LOCK_TENTATIVE,
        AuditLogMessageType.REINDEX_STARTED,
    ]:
        return False

    # Only REINDEX_STARTED messages have a heartbeat timestamp
    timestamp = (
        most_recent_doc["_source"].get("heartbeat_timestamp")
        or most_recent_doc["_source"]["timestamp"]
    )
    timestamp_dt = millis_timestamp_to_datetime(timestamp)
    return datetime.now() - timestamp_dt < LOCK_EXPIRATION_TIME


async def acquire_reindex_lock(
    provider: SearchProvider,
    index: str,
    *,
    dataset: str,
    dataset_version: str,
    reindex_type: AuditLogReindexType,
) -> bool:
    """Acquire a reindex lock for the given index by writing a REINDEX_STARTED message.

    Returns True if lock was acquired, False otherwise.
    """
    await ensure_audit_log_index(provider)

    # Check if there's already an active lock
    most_recent_doc = await _get_most_recent_audit_log_message(provider, index)
    if most_recent_doc:
        if _lock_is_active(most_recent_doc):
            log.debug(
                f"Found an active lock for index {index}, someone else is already reindexing"
            )
            return False

        log.debug(f"Found an expired lock for index {index}")

    # Write a tentative message
    tentative_lock_doc_id = await log_audit_message(
        provider,
        AuditLogMessageType.REINDEX_LOCK_TENTATIVE,
        index=index,
        dataset=dataset,
        dataset_version=dataset_version,
        reindex_type=reindex_type,
    )

    # Check if someone else wrote a message before us (race condition)
    # We choose the oldest tentative lock message as the winning one
    # as that's the most successful strategy when we assume write operations
    # to be processed in order.
    result = await provider.search(
        get_audit_log_index_name(),
        {
            "bool": {
                "must": [
                    {"term": {"index": index}},
                ]
            }
        },
        sort=[{"timestamp": {"order": "desc"}}],
        size=50,
    )

    hits = result.get("hits", {}).get("hits", [])
    oldest_tentative_lock_doc_id = None
    # We find the oldest tentative lock message in this series, i.e. before we find another message
    for hit in hits:
        if hit["_source"]["message_type"] != AuditLogMessageType.REINDEX_LOCK_TENTATIVE:
            break
        oldest_tentative_lock_doc_id = hit["_id"]

    # oldest_tentative_lock_doc_id = hits[0]["_id"] if hits else None
    if (
        oldest_tentative_lock_doc_id
        and oldest_tentative_lock_doc_id != tentative_lock_doc_id
    ):
        # Someone else wrote a message before us, we lost the race
        log.debug(
            f"Found an older tentative lock for index {index}, someone else won the race"
        )
        return False

    # Write the actual lock
    await log_audit_message(
        provider,
        AuditLogMessageType.REINDEX_STARTED,
        index=index,
        dataset=dataset,
        dataset_version=dataset_version,
        reindex_type=reindex_type,
    )

    return True


async def refresh_reindex_lock(
    provider: SearchProvider,
    index: str,
) -> bool:
    """Refresh the heartbeat timestamp of the current reindex lock."""
    await ensure_audit_log_index(provider)

    # Get the most recent message and check if it's a valid lock
    most_recent = await _get_most_recent_audit_log_message(provider, index)
    if not _lock_is_active(most_recent):
        log.warning(f"No valid reindex lock found for index {index}")
        return False

    # At this point, most_recent is guaranteed to be not None since _lock_is_active returned True
    assert most_recent is not None
    await provider.bulk_index(
        [
            {
                "_index": get_audit_log_index_name(),
                "_id": most_recent["_id"],
                "_op_type": "update",
                "doc": {
                    "heartbeat_timestamp": datetime_to_millis_timestamp(datetime.now()),
                },
            }
        ]
    )
    # Refresh the index to make the heartbeat timestamp visible
    await provider.refresh(get_audit_log_index_name())

    return True


async def release_reindex_lock(
    provider: SearchProvider,
    index: str,
    *,
    dataset: str,
    dataset_version: str,
    reindex_type: AuditLogReindexType,
    success: bool = True,
) -> bool:
    """Release the reindex lock by writing a completed/failed message."""
    # Check if the lock we're trying to release is still valid
    most_recent_doc = await _get_most_recent_audit_log_message(provider, index)
    if not _lock_is_active(most_recent_doc):
        log.warning(
            f"Attempting to release reindex lock for index {index}, but no valid lock found"
        )

    message_type = (
        AuditLogMessageType.REINDEX_COMPLETED
        if success
        else AuditLogMessageType.REINDEX_FAILED
    )

    await log_audit_message(
        provider,
        message_type,
        index=index,
        dataset=dataset,
        dataset_version=dataset_version,
        reindex_type=reindex_type,
    )

    return True
