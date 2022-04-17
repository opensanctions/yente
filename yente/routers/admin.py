import asyncio
import aiocron
import structlog
from structlog.stdlib import BoundLogger
from fastapi import APIRouter, Query
from fastapi import HTTPException

from yente import settings
from yente.data.common import ErrorResponse, HealthzResponse
from yente.search.search import get_index_status
from yente.search.indexer import update_index, update_index_threaded
from yente.search.base import close_es

log: BoundLogger = structlog.get_logger(__name__)
router = APIRouter()


async def regular_update():
    if settings.TESTING:
        return
    if not settings.AUTO_UPDATE:
        return
    update_index_threaded()


@router.on_event("startup")
async def startup_event():
    await regular_update()
    router.crontab = aiocron.crontab("*/30 * * * *", func=regular_update)


@router.on_event("shutdown")
async def shutdown_event():
    await close_es()


@router.get(
    "/healthz",
    summary="Health check",
    tags=["System information"],
    response_model=HealthzResponse,
    responses={500: {"model": ErrorResponse, "description": "Service is not ready"}},
)
async def healthz():
    """No-op basic health check. This is used by cluster management systems like
    Kubernetes to verify the service is responsive."""
    ok = await get_index_status()
    if not ok:
        raise HTTPException(500, detail="Index not ready")
    return HealthzResponse(status="ok")


@router.post(
    "/updatez",
    summary="Force an index update",
    tags=["System information"],
    response_model=HealthzResponse,
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
    return HealthzResponse(status="ok")
