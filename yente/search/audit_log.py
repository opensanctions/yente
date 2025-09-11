from enum import StrEnum
from datetime import datetime
from typing import Optional
from yente import logs, settings
from yente.provider.base import SearchProvider


# Query the audit log using the following query:
# curl -X GET "localhost:9200/yente-audit-log/_search" -H "Content-Type: application/json" -d '{"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "desc"}}], "size": 10000, "_source": true}' | jq '.hits.hits | map(._source) | reverse'

log = logs.get_logger(__name__)


def get_audit_log_index_name() -> str:
    return f"{settings.INDEX_NAME}-audit-log-{settings.AUDIT_LOG_INDEX_VERSION}"


def millis_timestamp_to_datetime(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000)


def datetime_to_millis_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class AuditLogEventType(StrEnum):
    # REINDEX_STARTED gets written as a lock, and is refreshed periodically.
    REINDEX_STARTED = "reindex_started"
    # REINDEX_COMPLETED or REINDEX_FAILED release the lock.
    REINDEX_COMPLETED = "reindex_completed"
    REINDEX_FAILED = "reindex_failed"
    CLEANUP_INDEX_DELETED = "cleanup_index_deleted"

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
                "event_type": {"type": "keyword"},
                "message": {"type": "text"},
                "timestamp": {"type": "date", "format": "epoch_millis"},
            }
        },
        settings={
            "number_of_shards": 1,
            "auto_expand_replicas": settings.INDEX_AUTO_REPLICAS,
        },
    )


async def log_audit_message(
    provider: SearchProvider,
    event_type: AuditLogEventType,
    *,
    index: str,
    dataset: Optional[str] = None,
    dataset_version: Optional[str] = None,
    message: str,
) -> str:
    """Log an audit message and return the document ID.

    An audit log message always concerns an index, as it is meant to record
    what data was made (un)available when. For this reason, `index` is a
    required argument.

    """

    timestamp = datetime_to_millis_timestamp(datetime.now())
    doc_id = f"{index}-{event_type}-{timestamp}"

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
                    "event_type": event_type,
                    "message": message,
                    "timestamp": timestamp,
                },
            }
        ]
    )

    return doc_id
