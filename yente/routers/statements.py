import structlog
from structlog.stdlib import BoundLogger
from typing import Optional
from fastapi import APIRouter, Query
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from yente import settings
from yente.models import StatementResponse
from yente.search.queries import statement_query
from yente.search.search import statement_results
from yente.util import limit_window
from yente.routers.util import get_dataset

log: BoundLogger = structlog.get_logger(__name__)
router = APIRouter()


@router.get(
    "/statements",
    summary="Statement-based records",
    tags=["Data access"],
    response_model=StatementResponse,
)
async def statements(
    dataset: Optional[str] = Query(None, title="Filter by dataset"),
    entity_id: Optional[str] = Query(None, title="Filter by source entity ID"),
    canonical_id: Optional[str] = Query(None, title="Filter by normalised entity ID"),
    prop: Optional[str] = Query(None, title="Filter by property name"),
    value: Optional[str] = Query(None, title="Filter by property value"),
    schema: Optional[str] = Query(None, title="Filter by schema type"),
    limit: int = Query(
        50,
        title="Number of results to return",
        lt=settings.MAX_PAGE,
    ),
    offset: int = Query(
        0,
        title="Number of results to skip before returning them",
        lt=settings.MAX_PAGE,
    ),
):
    """Access raw entity data as statements.

    Read [statement-based data model](https://www.opensanctions.org/docs/statements/)
    for context regarding this endpoint.
    """
    if not settings.STATEMENT_API:
        raise HTTPException(501, "Statement API not enabled.")
    ds = None
    if dataset is not None:
        ds = await get_dataset(dataset)
    query = statement_query(
        dataset=ds,
        entity_id=entity_id,
        canonical_id=canonical_id,
        prop=prop,
        value=value,
        schema=schema,
    )
    limit, offset = limit_window(limit, offset, 50)
    resp = await statement_results(query, limit, offset)
    return JSONResponse(content=resp, headers=settings.CACHE_HEADERS)
