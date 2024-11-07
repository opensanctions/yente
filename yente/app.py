import time
import aiocron  # type: ignore
from typing import AsyncGenerator, Dict, Type, Callable, Any, Coroutine, Union
from contextlib import asynccontextmanager
from pydantic import ValidationError
from fastapi import FastAPI
from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from fastapi.responses import JSONResponse
from structlog.contextvars import clear_contextvars, bind_contextvars

from yente import settings
from yente.exc import YenteError
from yente.logs import get_logger
from yente.routers import reconcile, search, match, admin
from yente.data import refresh_catalog
from yente.search.indexer import update_index_threaded
from yente.provider import close_provider
from yente.middleware import TraceContextMiddleware

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
    await close_provider()


async def request_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    start_time = time.time()
    client_ip = request.client.host if request.client else "127.0.0.1"
    bind_contextvars(
        client_ip=client_ip,
    )
    try:
        response = await call_next(request)
    except Exception as exc:
        log.exception("Exception during request: %s" % type(exc))
        response = JSONResponse(status_code=500, content={"status": "error"})
    time_delta = time.time() - start_time
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


async def yente_error_handler(req: Request, exc: YenteError) -> Response:
    if exc.status > 499:
        log.exception(f"App error {exc.status}: {exc.detail}")
    return JSONResponse(status_code=exc.status, content={"detail": exc.detail})


async def validation_error_handler(req: Request, exc: ValidationError) -> Response:
    log.warn(f"Validation error: {exc}")
    body = {"detail": exc.title, "errors": exc.errors()}
    return JSONResponse(status_code=400, content=body)


HANDLERS: Dict[Union[Type[Exception], int], ExceptionHandler] = {
    ValidationError: validation_error_handler,
    YenteError: yente_error_handler,
}


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.TITLE,
        description=settings.DESCRIPTION,
        version=settings.VERSION,
        contact=settings.CONTACT,
        openapi_tags=settings.TAGS,
        exception_handlers=HANDLERS,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.middleware("http")(request_middleware)
    app.add_middleware(TraceContextMiddleware)
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
