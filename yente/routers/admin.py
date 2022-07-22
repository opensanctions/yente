import aiocron
from fastapi import APIRouter, Query
from fastapi import HTTPException

from yente import settings
from yente.logs import get_logger
from yente.data import get_manifest, refresh_manifest
from yente.data.common import ErrorResponse, StatusResponse
from yente.data.manifest import Manifest
from yente.search.search import get_index_status
from yente.search.indexer import update_index, update_index_threaded
from yente.search.base import close_es

log = get_logger(__name__)
router = APIRouter()


async def regular_update():
    if settings.TESTING:
        return
    update_index_threaded()


@router.on_event("startup")
async def startup_event():
    await get_index_status()
    manifest = await get_manifest()
    if settings.MANIFEST_CRONTAB:
        router.cron_refresh = aiocron.crontab(
            settings.MANIFEST_CRONTAB,
            func=refresh_manifest,
        )
    if manifest.schedule is not None:
        await regular_update()
        router.cron_update = aiocron.crontab(manifest.schedule, func=regular_update)


@router.on_event("shutdown")
async def shutdown_event():
    await close_es()


@router.get(
    "/healthz",
    summary="Health check",
    tags=["System information"],
    response_model=StatusResponse,
    responses={500: {"model": ErrorResponse, "description": "Service is not ready"}},
)
async def healthz():
    """No-op basic health check. This is used by cluster management systems like
    Kubernetes to verify the service is responsive."""
    return StatusResponse(status="ok")


@router.get(
    "/readyz",
    summary="Search index readiness check",
    tags=["System information"],
    response_model=StatusResponse,
    responses={503: {"model": ErrorResponse, "description": "Index is not ready"}},
)
async def readyz():
    """Search index health check. This is used to know if the service has completed its index building."""
    ok = await get_index_status(index=settings.ENTITY_INDEX)
    if not ok:
        raise HTTPException(503, detail="Index not ready.")
    return StatusResponse(status="ok")


@router.get(
    "/manifest",
    summary="Dataset manifest",
    tags=["System information"],
    response_model=Manifest,
)
async def manifest():
    """Return the service manifest, including the list of all indexed datasets."""
    return await get_manifest()


@router.post(
    "/updatez",
    summary="Force an index update",
    tags=["System information"],
    response_model=StatusResponse,
    responses={403: {"model": ErrorResponse, "description": "Authorization error."}},
)
async def force_update(
    token: str = Query("", title="Update token for authentication"),
    sync: bool = Query(False, title="Wait until indexing is complete"),
):
    """Force the index to be re-generated. Works only if the update token is provided
    (serves as an API key, and can be set in the container environment)."""
    if (
        not len(token.strip())
        or settings.UPDATE_TOKEN is None
        or not len(settings.UPDATE_TOKEN)
        or token != settings.UPDATE_TOKEN
    ):
        raise HTTPException(403, detail="Invalid token.")
    if sync:
        await update_index(force=True)
    else:
        update_index_threaded(force=True)
    return StatusResponse(status="ok")
