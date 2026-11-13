from fastapi import APIRouter, Request, Response

from yente.data.common import ErrorResponse
from yente.data.match_v2 import EntityMatches, EntityMatchQuery
from yente.logs import get_logger
from yente.routers.util import ProviderDep

log = get_logger(__name__)
router = APIRouter(prefix="/v2")


@router.post(
    "/match",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatches,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def match_v2(
    request: Request,
    response: Response,
    provider: ProviderDep,
    match: EntityMatchQuery,
) -> EntityMatches:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person."""
    raise NotImplementedError()
