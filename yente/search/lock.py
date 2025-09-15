from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import uuid
from yente import logs, settings
from yente.exc import YenteIndexError
from yente.provider.base import SearchProvider


LOCK_EXPIRATION_TIME = timedelta(minutes=10)
# We use a single lock document for everything, this could be expanded to support more granular lockin in the future.
# Currently, there is only one lock document for the entire settings.INDEX_NAME.
LOCK_DOC_ID = "lock"

log = logs.get_logger(__name__)


@dataclass(frozen=True)
class LockSession:
    id: str

    @classmethod
    def create(cls) -> "LockSession":
        return cls(id=str(uuid.uuid4()))


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


def lock_is_active(hit: Dict[str, Any]) -> bool:
    return (
        datetime.now() - from_millis_timestamp(hit["_source"]["acquired_at"])
        < LOCK_EXPIRATION_TIME
    )


async def acquire_lock(provider: SearchProvider) -> Optional[LockSession]:
    """Acquire the global lock for the entire settings.INDEX_NAME prefix.

    Returns a LockSession if the lock was acquired, None if it was not.
    The LockSession must be passed the refresh and release the lock.

    Non-blocking, returns True if the lock was acquired, False if it was not.
    """
    # Create the lock index if it doesn't exist
    await ensure_lock_index(provider)

    # The lock session we will be trying to acquire
    lock_session = LockSession.create()

    # The general idea here is:
    # 1. Try to insert the lock, expecting it to fail if the lock is already present
    # 2. If the document is already present, check if it's expired
    # 3. If it's expired, overwrite it
    # 4. If it's not expired, return None

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
                        "lock_session_id": lock_session.id,
                    },
                }
            ]
        )
        # We succeeded in acquiring the lock
        log.info(f"Acquired lock {lock_session.id}")
        return lock_session
    except YenteIndexError as e:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error. But that's the intention here.
        log.debug(
            f"Lock already exists, next we check if it's expired before trying to overwrite. Response: {e}"
        )
        pass

    # Second phase: get the lock, check if it's expired, overwrite if it is..
    try:
        hit = await provider.get_document(get_lock_index_name(), LOCK_DOC_ID)
        if not hit:
            log.warning(
                "First we failed to create the lock document, but now we can't find the lock document - that's weird."
            )
            return None
        found_lock_session_id = hit["_source"]["lock_session_id"]

        if lock_is_active(hit):
            # The lock is still valid,
            log.debug(
                f"Found a non-expired lock held by {found_lock_session_id}, acquiring lock failed!"
            )
            return None
        else:
            # The lock is expired, probably another process failed to clean it up
            log.debug(
                f"Found expired lock session {found_lock_session_id}, probably another process failed to clean it up, acquiring lock"
            )
            pass

        update_op = {
            "_op_type": "update",
            "_index": get_lock_index_name(),
            "_id": LOCK_DOC_ID,
            # doc (instead of _source)is used for the partial update
            "doc": {
                "acquired_at": to_millis_timestamp(datetime.now()),
                "lock_session_id": lock_session.id,
            },
            # Uses seq_no and primary_term to prevent race conditions.
            # seq_no tracks the document's version across all shards, while primary_term ensures
            # we're updating the document on the same primary shard that last modified it. See
            # https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control
            # for more information.
            # If two processes write the lock, this will cause one of them to fail with a conflict error.
            "if_seq_no": hit["_seq_no"],
            "if_primary_term": hit["_primary_term"],
        }

        await provider.bulk_index([update_op])

        # No conflict error, so we succeeded in acquiring the lock
        log.info(f"Acquired lock {lock_session.id}")
        return lock_session

    except YenteIndexError as e:
        # NOTE: Because it's a bulk operation (to keep the provider interface lean),
        # we don't get detailed error information, so the error we're catching isn't
        # guaranteed to be a conflict error.
        # If it's a conflict error, someone beat us to it (i.e. someone else acquired the lock)
        # after we read it and the write failed because of our (_seq_no, _primary_term)
        log.debug(
            f"Failed to update lock, probably someone else acquired it before us. Response: {e}"
        )
        return None


async def release_lock(provider: SearchProvider, lock_session: LockSession) -> None:
    """Release a lock for the given index by deleting the lock document.

    Uses bulk_index with delete operation to remove the lock document.
    """
    log.debug(f"Releasing lock {lock_session.id}")
    # First get the lock document, just in case someone else acquired it. This should
    # never happen, but in case we're releasing a lock late here because of a bug, we
    # don't want to mess with other processes' locks.
    try:
        hit = await provider.get_document(get_lock_index_name(), LOCK_DOC_ID)
        if not hit:
            return
        found_lock_session_id = hit["_source"]["lock_session_id"]
        if found_lock_session_id != lock_session.id:
            log.error(
                f"Trying to release lock {lock_session.id}, but found {found_lock_session_id}."
                "Not releasing lock since it's not the one we were expecting to release."
            )
            return
    except YenteIndexError as e:
        log.warning(
            f"Elasticsearch error when getting lock document, will still try to delete it. Response: {e}"
        )

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
        log.info(f"Released lock {lock_session.id}")
    except YenteIndexError as e:
        log.error(f"Failed to release lock {lock_session.id}. Response: {e}")


async def refresh_lock(provider: SearchProvider, lock_session: LockSession) -> bool:
    """Refresh a lock by updating the acquired_at time to now.

    Assumes the lock is already being held by the caller.
    Returns True if the lock was successfully refreshed, False otherwise.
    """
    log.debug(f"Refreshing lock {lock_session.id}")
    try:
        # Get the current lock document to check if everything is okay
        hit = await provider.get_document(get_lock_index_name(), LOCK_DOC_ID)
        if not hit:
            return False
        found_lock_session_id = hit["_source"]["lock_session_id"]

        if found_lock_session_id != lock_session.id:
            # We're trying to refresh a lock, but it's no longer ours!
            # We probably let it expire and someone else got to it!
            log.error(
                f"Found a lock held by {found_lock_session_id}, refreshing lock failed!",
                found_lock_session_id=found_lock_session_id,
                found_lock_acquired_at=from_millis_timestamp(
                    hit["_source"]["acquired_at"]
                ),
            )
            return False

        if not lock_is_active(hit):
            # It's our lock, but we let it expire! Here be dragons, bail out.
            return False

        # Prepare the update operation - only update acquired_at field, we're already holding the lock
        update_op = {
            "_op_type": "update",
            "_index": get_lock_index_name(),
            "_id": LOCK_DOC_ID,
            # doc (instead of _source)is used for the partial update
            "doc": {"acquired_at": to_millis_timestamp(datetime.now())},
            # We already verified that we're the ones holding this lock, so we don't expect to have to
            # use the optimistic concurrency control here. But just in case, it doesn't hurt either.
            "if_seq_no": hit["_seq_no"],
            "if_primary_term": hit["_primary_term"],
        }

        await provider.bulk_index([update_op])
        log.info(f"Refreshed lock {lock_session.id}")
        return True

    except YenteIndexError as e:
        # We don't expect this to ever happen, since we expected to be the ones holding the lock and
        # therefore not racing others to it. But just in case, bail out.
        log.error(
            f"Failed to refresh lock, even though we expected to be the only ones writing to it. Response: {e}"
        )
        return False
