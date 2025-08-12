from datetime import datetime, timedelta
from yente import logs, settings
from yente.exc import YenteIndexError
from yente.provider.base import SearchProvider


LOCK_EXPIRATION_TIME = timedelta(minutes=5)

log = logs.get_logger(__name__)


def get_lock_index_name() -> str:
    return f"{settings.ENTITY_INDEX}-locks"


async def ensure_lock_index(provider: SearchProvider) -> None:
    if get_lock_index_name() in await provider.get_all_indices():
        return

    await provider.create_index(
        get_lock_index_name(),
        mappings={
            "properties": {
                "index": {"type": "keyword"},
                # date has millisecond accuracy, which is enough for our use case
                "acquired_at": {"type": "date", "format": "epoch_millis"},
            }
        },
        settings={
            # Single shard eliminates cross-shard consistency issues (but not split-brain
            # issues, which is what _primary_term is for)
            "number_of_shards": 1,
            "auto_expand_replicas": settings.INDEX_AUTO_REPLICAS,
        },
    )


def to_millis_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def from_millis_timestamp(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000)


async def acquire_lock(provider: SearchProvider, index: str) -> bool:
    """Acquire a lock for the given index and type.

    Non-blocking, returns True if the lock was acquired, False if it was not.
    """
    # Create the lock index if it doesn't exist
    await ensure_lock_index(provider)

    try:
        # Naively try to insert the lock, expecting it to fail if the document is already present
        await provider.bulk_index(
            [
                {
                    "_index": get_lock_index_name(),
                    "_id": f"{index}",
                    # Create is important here, otherwise we won't get the exception on conflict
                    "_op_type": "create",
                    "_source": {
                        "index": index,
                        "acquired_at": to_millis_timestamp(datetime.now()),
                    },
                }
            ]
        )
    except YenteIndexError:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error. But that's the intention here.
        return await _overwrite_lock(provider, index, only_if_expired=True)

    return True


async def _overwrite_lock(
    provider: SearchProvider, index: str, only_if_expired: bool
) -> bool:
    """Attempt to acquire a lock using optimistic concurrency control.

    Note that this method is used for both acquiring an expired lock (if only_if_expired is True)
    and refreshing a lock (if only_if_expired is False). Refreshing a lock is required because reindex
    operations often take longer than the lock expiration time. When refreshing but the lock
    is expired, we only warn.

    Uses seq_no and primary_term to prevent race conditions.
    seq_no tracks the document's version across all shards, while primary_term ensures
    we're updating the document on the same primary shard that last modified it. See
    https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control
    for more information.
    """
    try:
        # Get the current lock document to check if it's expired
        # Needs get_document because the _seq_no and _primary_term
        # are not returned by search.
        hit = await provider.get_document(get_lock_index_name(), index)
        if not hit:
            return False

        acquired_at = from_millis_timestamp(hit["_source"]["acquired_at"])

        if datetime.now() - acquired_at < LOCK_EXPIRATION_TIME:
            # The lock is still valid
            if only_if_expired:
                # ...and only_if_expired is True, so we won't overwrite it
                # This is the "other process holding lock" case
                return False
        else:
            # The lock is expired, probably another process failed to clean it up
            if only_if_expired:
                log.debug(f"Acquiring expired lock for {index}")
            else:
                # ...and only_if_expired is False, so we're refreshing a lock
                # expecting to already hold it. But it's expired, so we called the lock refresh
                # too late!
                log.warning(f"Refreshing lock for {index}, but it's already expired!")

        # Prepare the update operation - only update acquired_at field
        # Use doc to update specific fields
        update_op = {
            "_op_type": "update",
            "_index": get_lock_index_name(),
            "_id": index,
            # doc (instead of _source)is used for the partial update
            "doc": {"acquired_at": to_millis_timestamp(datetime.now())},
            # optimistic concurrency control to prevent a race condition
            # where two processes write the lock and both think they succeeded.
            # This will cause one of them to fail with a conflict error.
            "if_seq_no": hit["_seq_no"],
            "if_primary_term": hit["_primary_term"],
        }

        await provider.bulk_index([update_op])

        # No conflict error, so we succeeded in acquiring the lock
        return True

    except YenteIndexError:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error.
        # If it's a conflict error, someone beat us to it (i.e. someone else acquired the lock)
        # after we read it and the write failed because of our (_seq_no, _primary_term)
        return False


async def release_lock(provider: SearchProvider, index: str) -> None:
    """Release a lock for the given index by deleting the lock document.

    Uses bulk_index with delete operation to remove the lock document.
    """
    log.debug(f"Releasing lock for {index}")
    try:
        await provider.bulk_index(
            [
                {
                    "_op_type": "delete",
                    "_index": get_lock_index_name(),
                    "_id": index,
                }
            ]
        )
    except YenteIndexError:
        log.warning(
            f"Failed to release lock for {index}, maybe it was already released or never acquired?"
        )


async def refresh_lock(provider: SearchProvider, index: str) -> bool:
    """Refresh a lock by updating the acquired_at time to now.

    Assumes the lock is already being held by the caller.
    Returns True if the lock was successfully refreshed, False otherwise.
    """
    # We don't want to check if it's expired, in fact we assume it's not.
    log.debug(f"Refreshing lock for {index}")
    return await _overwrite_lock(provider, index, only_if_expired=False)
