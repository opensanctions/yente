import hashlib
from typing import List, Optional, Union
from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi import HTTPException
from fastapi.openapi.docs import get_redoc_html
from fastapi.responses import FileResponse, HTMLResponse
from normality import squash_spaces

from yente import settings
from yente.logs import get_logger
from yente.data import get_catalog
from yente.data.common import ErrorResponse, StatusResponse
from yente.data.common import DataCatalogModel, AlgorithmResponse, Algorithm
from yente.data.manifest import Catalog
from yente.provider import SearchProvider, get_provider
from yente.routers.util import ENABLED_ALGORITHMS
from yente.search.indexer import update_index, update_index_threaded
from yente.search.status import sync_dataset_versions

log = get_logger(__name__)
router = APIRouter()


def catalog_etag(catalog: Catalog) -> str:
    """Return a strong ETag for the catalog response.

    The response body is fully determined by each dataset's name, version and
    currently-indexed version, so a hash over those triples changes exactly
    when the response would. This avoids serialising the body just to compare.
    """
    parts = [
        "%s:%s:%s" % (ds.name, ds.model.version, ds.model.index_version)
        for ds in catalog.datasets
    ]
    # Only a cache validator, not a security primitive.
    digest = hashlib.sha1(
        "\n".join(sorted(parts)).encode("utf-8"), usedforsecurity=False
    ).hexdigest()
    return '"%s"' % digest


def etag_matches(header: Optional[str], etag: str) -> bool:
    """Check whether an ``If-None-Match`` header matches the given ETag.

    Supports the ``*`` wildcard and a comma-separated list of ETags.
    """
    if header is None:
        return False
    candidates = [tag.strip() for tag in header.split(",")]
    return "*" in candidates or etag in candidates


@router.get(
    "/",
    summary="ReDoc API documentation",
    include_in_schema=False,
)
async def redoc_html() -> HTMLResponse:
    """Render the ReDoc API documentation page."""
    response = get_redoc_html(
        openapi_url="/openapi.json",
        title=settings.TITLE,
        redoc_js_url=settings.REDOC_JS_URL,
        redoc_favicon_url="https://assets.opensanctions.org/images/nura/favicon-32.png",
        with_google_fonts=False,
    )
    # get_redoc_html produces a plain <script src="..."> tag with no integrity check.
    # We inject Subresource Integrity (SRI) attributes so the browser verifies the
    # script's SHA-384 hash before executing it. If the server ever delivers a different
    # file the hash won't match and the browser refuses to run it.
    # Update YENTE_REDOC_JS_URL and YENTE_REDOC_JS_SRI together when upgrading ReDoc.
    # onerror fires on SRI mismatch or network failure (unlike <noscript>, which only
    # fires when JavaScript is disabled entirely).
    fallback = (
        '<p id="redoc-error" style="display:none;padding:2em">'
        "If you see this, you may need to update <code>YENTE_REDOC_JS_SRI</code> "
        "to match the script at <code>YENTE_REDOC_JS_URL</code>.</p>"
    )
    html = (
        bytes(response.body)
        .decode("utf-8")
        .replace(
            f'src="{settings.REDOC_JS_URL}"',
            f'src="{settings.REDOC_JS_URL}" integrity="{settings.REDOC_JS_SRI}"'
            ' crossorigin="anonymous"'
            ' onerror="document.getElementById(\'redoc-error\').style.display=\'block\'"',
        )
        .replace("<redoc ", f"{fallback}\n    <redoc ")
    )
    return HTMLResponse(html)


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
    responses={304: {"description": "The catalog has not changed."}},
)
@router.get(
    "/manifest",
    response_model=DataCatalogModel,
    include_in_schema=False,
)
async def catalog(
    request: Request,
    response: Response,
    provider: SearchProvider = Depends(get_provider),
) -> Union[Response, DataCatalogModel]:
    """Return the service manifest, which includes a list of all indexed datasets.

    The manifest is the configuration file of the yente service. It specifies what
    data sources are included, and how often they should be loaded.
    """
    catalog = await get_catalog()
    await sync_dataset_versions(provider, catalog)
    etag = catalog_etag(catalog)
    headers = {"ETag": etag, "Cache-Control": "public, max-age=300"}
    if etag_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)
    response.headers.update(headers)
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
            docs=algo.get_docs(),
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
        settings.UPDATE_TOKEN is None
        or len(settings.UPDATE_TOKEN) == 0
        or len(token.strip()) == 0
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
