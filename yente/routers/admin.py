from typing import List
from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException
from fastapi.openapi.docs import get_redoc_html
from fastapi.responses import FileResponse, HTMLResponse
from normality import squash_spaces

from yente import settings
from yente.logs import get_logger
from yente.data import get_catalog
from yente.data.common import ErrorResponse, StatusResponse
from yente.data.common import DataCatalogModel, AlgorithmResponse, Algorithm
from yente.provider import SearchProvider, get_provider
from yente.routers.util import ENABLED_ALGORITHMS
from yente.search.indexer import update_index, update_index_threaded
from yente.search.status import sync_dataset_versions

log = get_logger(__name__)
router = APIRouter()


@router.get(
    "/",
    summary="ReDoc API documentation",
    include_in_schema=False,
)
async def redoc_html() -> HTMLResponse:
    """Render the ReDoc API documentation renderer script."""
    return get_redoc_html(
        openapi_url="/openapi.json",
        title=settings.TITLE,
        redoc_js_url="https://assets.opensanctions.org/scripts/redoc.standalone.js",
        redoc_favicon_url="https://assets.opensanctions.org/images/favicon-32x32.png",
        with_google_fonts=False,
    )


@router.get(
    "/healthz",
    summary="Health check",
    tags=["System information"],
    response_model=StatusResponse,
    responses={500: {"model": ErrorResponse, "description": "Service is not ready"}},
)
async def healthz() -> StatusResponse:
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
async def readyz(
    provider: SearchProvider = Depends(get_provider),
) -> StatusResponse:
    """Search index health check. This is used to know if the service has completed
    its index building."""
    ok = await provider.check_health(index=settings.ENTITY_INDEX)
    if not ok:
        raise HTTPException(503, detail="Index not ready.")
    return StatusResponse(status="ok")


@router.get(
    "/catalog",
    summary="Data catalog",
    tags=["Data access"],
    response_model=DataCatalogModel,
)
@router.get(
    "/manifest",
    response_model=DataCatalogModel,
    include_in_schema=False,
)
async def catalog(
    provider: SearchProvider = Depends(get_provider),
) -> DataCatalogModel:
    """Return the service manifest, which includes a list of all indexed datasets.

    The manifest is the configuration file of the yente service. It specifies what
    data sources are included, and how often they should be loaded.
    """
    catalog = await get_catalog()
    await sync_dataset_versions(provider, catalog)
    model = DataCatalogModel(datasets=[], current=[], outdated=[])
    for dataset in catalog.datasets:
        if dataset.model.load and dataset.model.index_current:
            model.current.append(dataset.name)
        elif dataset.model.index_version is not None:
            model.outdated.append(dataset.name)
        dataset.model.children = set([c.name for c in dataset.children])
        model.datasets.append(dataset.model)
    model.index_stale = len(model.outdated) > 0
    return model


@router.get(
    "/algorithms",
    tags=["System information"],
    response_model=AlgorithmResponse,
)
async def algorithms() -> AlgorithmResponse:
    """Return a list of the supported matching/scoring algorithms used by the matching
    endpoint.

    See also the [scoring documentation](https://www.opensanctions.org/docs/api/scoring/).
    """
    algorithms: List[Algorithm] = []
    for algo in ENABLED_ALGORITHMS:
        if algo.NAME in settings.HIDDEN_ALGORITHMS:
            continue
        desc = Algorithm(
            name=algo.NAME,
            description=squash_spaces(algo.__doc__ or ""),
            features=algo.get_feature_docs(),
        )
        algorithms.append(desc)
    return AlgorithmResponse(
        algorithms=algorithms,
        default=settings.DEFAULT_ALGORITHM,
        best=settings.BEST_ALGORITHM,
    )


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
) -> StatusResponse:
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


@router.get(
    "/favicon.ico",
    summary="Browser tab bar icon",
    include_in_schema=False,
)
async def favicon() -> FileResponse:
    """Browser tab bar icon."""
    return FileResponse(settings.RESOURCES_PATH / "favicon.ico")
