import time
import aiocron  # type: ignore
from uuid import uuid4
from typing import AsyncGenerator, Dict, Type, Callable, Any, Coroutine, Union
from contextlib import asynccontextmanager
from elasticsearch import ApiError, TransportError
from pydantic import ValidationError
from fastapi import FastAPI
from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from fastapi.responses import JSONResponse
from structlog.contextvars import clear_contextvars, bind_contextvars

from yente import settings
from yente.logs import get_logger
from yente.routers import reconcile, search, match, admin
from yente.data import refresh_catalog
from yente.search.base import close_es
from yente.search.indexer import update_index_threaded

log = get_logger("yente")
ExceptionHandler = Callable[[Request, Any], Coroutine[Any, Any, Response]]


async def cron_task() -> None:
    await refresh_catalog()
    if settings.AUTO_REINDEX:
        update_index_threaded()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    log.info(
        "Setting up background refresh",
        crontab=settings.CRONTAB,
        auto_reindex=settings.AUTO_REINDEX,
    )
    settings.CRON = aiocron.crontab(settings.CRONTAB, func=cron_task)
    if settings.AUTO_REINDEX:
        update_index_threaded()
    yield
    await close_es()


async def request_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    start_time = time.time()
    trace_id = request.headers.get("x-trace-id")
    if trace_id is None:
        trace_id = uuid4().hex
    client_ip = request.client.host if request.client else "127.0.0.1"
    bind_contextvars(
        trace_id=trace_id,
        client_ip=client_ip,
    )
    try:
        response = await call_next(request)
    except Exception as exc:
        log.exception("Exception during request: %s" % type(exc))
        response = JSONResponse(status_code=500, content={"status": "error"})
    time_delta = time.time() - start_time
    response.headers["x-trace-id"] = trace_id
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


async def api_error_handler(request: Request, exc: ApiError) -> Response:
    log.exception(f"Search error {exc.status_code}: {exc.message}")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


async def transport_error_handler(request: Request, exc: TransportError) -> Response:
    log.exception(f"Transport: {exc.message}")
    return JSONResponse(status_code=500, content={"detail": exc.message})


async def validation_error_handler(request: Request, exc: ValidationError) -> Response:
    log.warn(f"Validation error: {exc}")
    body = {"detail": exc.title, "errors": exc.errors()}
    return JSONResponse(status_code=400, content=body)


HANDLERS: Dict[Union[Type[Exception], int], ExceptionHandler] = {
    ApiError: api_error_handler,
    TransportError: transport_error_handler,
    ValidationError: validation_error_handler,
}


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.TITLE,
        description=settings.DESCRIPTION,
        version=settings.VERSION,
        contact=settings.CONTACT,
        openapi_tags=settings.TAGS,
        exception_handlers=HANDLERS,
        redoc_url="/",
        lifespan=lifespan,
    )
    app.middleware("http")(request_middleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=500)
    app.include_router(match.router)
    app.include_router(search.router)
    app.include_router(reconcile.router)
    app.include_router(admin.router)
    return app
