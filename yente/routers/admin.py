import asyncio
import structlog
from structlog.stdlib import BoundLogger
from fastapi import APIRouter, Query
from fastapi import HTTPException, BackgroundTasks

from yente import settings
from yente.models import HealthzResponse
from yente.search.search import get_index_status
from yente.search.indexer import update_index
from yente.search.base import get_es

log: BoundLogger = structlog.get_logger(__name__)
router = APIRouter()


@router.on_event("startup")
async def startup_event():
    asyncio.create_task(update_index())


@router.on_event("shutdown")
async def shutdown_event():
    es = await get_es()
    await es.close()


@router.get(
    "/healthz",
    summary="Health check",
    tags=["System information"],
    response_model=HealthzResponse,
)
async def healthz():
    """No-op basic health check. This is used by cluster management systems like
    Kubernetes to verify the service is responsive."""
    ok = await get_index_status()
    if not ok:
        raise HTTPException(500, detail="Index not ready")
    return {"status": "ok"}


@router.post(
    "/updatez",
    summary="Force an index update",
    tags=["System information"],
    response_model=HealthzResponse,
)
async def force_update(
    background_tasks: BackgroundTasks,
    token: str = Query("", title="Update token for authentication"),
    sync: bool = Query(False, title="Wait until indexing is complete"),
):
    """Force the index to be re-generated. Works only if the update token is provided
    (serves as an API key, and can be set in the container environment)."""
    if not len(token.strip()) or not len(settings.UPDATE_TOKEN):
        raise HTTPException(403, detail="Invalid token.")
    if token != settings.UPDATE_TOKEN:
        raise HTTPException(403, detail="Invalid token.")
    if sync:
        await update_index(force=True)
    else:
        background_tasks.add_task(update_index, force=True)
    return {"status": "ok"}
