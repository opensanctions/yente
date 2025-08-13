from enum import StrEnum
from datetime import datetime
from yente import settings
from yente.provider.base import SearchProvider
from yente.search.lock import to_millis_timestamp


# Query the audit log using the following query:
# curl -X GET "localhost:9200/yente-entities-audit-log/_search" -H "Content-Type: application/json" -d '{"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "desc"}}], "size": 10000, "_source": true}' | jq '.hits.hits | map(._source) | reverse'


def get_audit_log_index_name() -> str:
    return f"{settings.ENTITY_INDEX}-audit-log"


class AuditLogReindexType(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


class AuditLogMessageType(StrEnum):
    REINDEX_STARTED = "reindex_started"
    REINDEX_COMPLETED = "reindex_completed"
    REINDEX_FAILED = "reindex_failed"
    INDEX_ALIAS_ROLLOVER_COMPLETE = "index_alias_rollover_complete"
    # TODO: REINDEX_PROGRESS?


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
) -> None:
    """Log an audit message to the audit log index."""
    await ensure_audit_log_index(provider)

    timestamp = to_millis_timestamp(datetime.now())
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
