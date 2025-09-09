from datetime import datetime, timedelta
from yente import logs, settings
from yente.exc import YenteIndexError
from yente.provider.base import SearchProvider


LOCK_EXPIRATION_TIME = timedelta(minutes=5)
# We use a single lock document for everything, this could be expanded to support more granular lockin in the future.
# Currently, there is only one lock document for the entire settings.INDEX_NAME.
LOCK_DOC_ID = "lock"

log = logs.get_logger(__name__)


def get_lock_index_name() -> str:
    return f"{settings.INDEX_NAME}-locks"


async def ensure_lock_index(provider: SearchProvider) -> None:
    if get_lock_index_name() in await provider.get_all_indices():
        return

    await provider.create_index(
        get_lock_index_name(),
        mappings={
            "properties": {
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


async def acquire_lock(provider: SearchProvider) -> bool:
    """Acquire the global lock for the entire settings.INDEX_NAME prefix.

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
                    "_id": LOCK_DOC_ID,
                    # create is important here, otherwise we won't get the exception on conflict
                    "_op_type": "create",
                    "_source": {
                        "acquired_at": to_millis_timestamp(datetime.now()),
                    },
                }
            ]
        )
    except YenteIndexError as e:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error. But that's the intention here.
        log.debug(
            f"Lock already exists, will try to overwrite, but only if expired. Response: {e}"
        )
        return await _overwrite_lock(provider, only_if_expired=True)

    return True


async def _overwrite_lock(provider: SearchProvider, only_if_expired: bool) -> bool:
    """Attempt to acquire a lock using optimistic concurrency control.

    Note that this method is used for both acquiring an expired lock (if only_if_expired is True)
    and refreshing a lock (if only_if_expired is False). Refreshing a lock is required because reindex
    operations often take longer than the lock expiration time. When refreshing but the lock
    is expired, we warn and proceed with overwriting the lock

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
        hit = await provider.get_document(get_lock_index_name(), LOCK_DOC_ID)
        if not hit:
            return False

        acquired_at = from_millis_timestamp(hit["_source"]["acquired_at"])

        if datetime.now() - acquired_at < LOCK_EXPIRATION_TIME:
            # The lock is still valid
            if only_if_expired:
                # ...and only_if_expired is True, so we won't overwrite it
                # This is the "other process holding lock" case
                log.debug("Found a non-expired lock, not overwriting")
                return False
        else:
            # The lock is expired, probably another process failed to clean it up
            if only_if_expired:
                log.debug("Acquiring expired lock")
            else:
                # ...and only_if_expired is False, so we're refreshing a lock
                # expecting to already hold it. But it's expired, so we called the lock refresh
                # too late!
                log.warning("Refreshing lock, but it's already expired!")
                return False

        # Prepare the update operation - only update acquired_at field
        update_op = {
            "_op_type": "update",
            "_index": get_lock_index_name(),
            "_id": LOCK_DOC_ID,
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

    except YenteIndexError as e:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error.
        # If it's a conflict error, someone beat us to it (i.e. someone else acquired the lock)
        # after we read it and the write failed because of our (_seq_no, _primary_term)
        log.debug(
            f"Failed to update lock, probably someone else acquired it before us. Response: {e}"
        )
        return False


async def release_lock(provider: SearchProvider) -> None:
    """Release a lock for the given index by deleting the lock document.

    Uses bulk_index with delete operation to remove the lock document.
    """
    log.debug("Releasing lock")
    try:
        await provider.bulk_index(
            [
                {
                    "_op_type": "delete",
                    "_index": get_lock_index_name(),
                    "_id": LOCK_DOC_ID,
                }
            ]
        )
    except YenteIndexError as e:
        log.warning(
            f"Failed to release lock, maybe it was already released or never acquired? Response: {e}"
        )


async def refresh_lock(provider: SearchProvider) -> bool:
    """Refresh a lock by updating the acquired_at time to now.

    Assumes the lock is already being held by the caller.
    Returns True if the lock was successfully refreshed, False otherwise.
    """
    log.debug("Refreshing lock")
    # We don't want to check if it's expired, in fact we assume it's not.
    return await _overwrite_lock(provider, only_if_expired=False)
