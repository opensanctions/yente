import time
from uuid import uuid4
from elasticsearch import ApiError, TransportError
from normality import slugify
from typing import Optional, cast
from starlette.requests import Headers
from fastapi import FastAPI
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from structlog.contextvars import clear_contextvars, bind_contextvars

from yente import settings
from yente.logs import get_logger
from yente.routers import reconcile, search, statements, admin

log = get_logger("yente")
app = FastAPI(
    title=settings.TITLE,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    contact=settings.CONTACT,
    openapi_tags=settings.TAGS,
    redoc_url="/",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(search.router)
app.include_router(reconcile.router)
app.include_router(statements.router)
app.include_router(admin.router)


def get_user_id(headers: Headers) -> Optional[str]:
    """Get the user identifiers from headers. User identifiers are just
    telemetry tools, not authorization mechanisms."""
    user_id = headers.get("authorization")
    if user_id is not None:
        if " " in user_id:
            _, user_id = user_id.split(" ", 1)
        user_id = slugify(user_id)
    if user_id is not None:
        user_id = user_id[:40]
    return user_id


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start_time = time.time()
    user_id = get_user_id(request.headers)
    trace_id = uuid4().hex
    bind_contextvars(
        user_id=user_id,
        trace_id=trace_id,
        client_ip=request.client.host,
    )
    try:
        response = cast(Response, await call_next(request))
    except Exception as exc:
        log.exception("Exception during request: %s" % type(exc))
        response = JSONResponse(status_code=500, content={"status": "error"})
    time_delta = time.time() - start_time
    response.headers["x-trace-id"] = trace_id
    if user_id is not None:
        response.headers["x-user-id"] = user_id
    log.info(
        str(request.url.path),
        action="request",
        method=request.method,
        path=request.url.path,
        query=request.url.query,
        agent=request.headers.get("user-agent"),
        referer=request.headers.get("referer"),
        code=response.status_code,
        took=time_delta,
    )
    clear_contextvars()
    return response


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    log.error(f"Search error {exc.status_code}: {exc.message}")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.exception_handler(TransportError)
async def transport_error_handler(request: Request, exc: TransportError):
    log.error(f"Transport: {exc.message}")
    return JSONResponse(status_code=500, content={"detail": exc.message})
