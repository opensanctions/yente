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

from yente import settings
from yente.exc import YenteError
from yente.logs import get_logger
from yente.routers import reconcile, search, match, admin
from yente.data import refresh_catalog
from yente.data.entity import Entity
from yente.routers.util import ENABLED_ALGORITHMS
from yente.search.indexer import update_index_threaded
from yente.provider import close_provider
from yente.middleware import RequestLogMiddleware, TraceContextMiddleware

log = get_logger("yente")
ExceptionHandler = Callable[[Request, Any], Coroutine[Any, Any, Response]]


async def cron_task() -> None:
    await refresh_catalog()
    if settings.AUTO_REINDEX:
        update_index_threaded()


async def warm_up() -> None:
    """Warm up the application by loading the catalog and updating the index."""
    log.info("Warming up application...")
    await refresh_catalog()
    if settings.AUTO_REINDEX:
        update_index_threaded()

    log.debug("Warming up matcher algorithms...")
    # This is the pragmatic and easy way to warm up the matcher algorithms. If anyone feels the call to
    # do it the elegant way, implement a warm up function in each of the algorithms that in turn calls
    # some eager loading functions in rigour, don't let this stop you, hack away!
    fake_person = Entity.from_dict(
        {
            "schema": "Person",
            "id": "warm-up-person",
            "properties": {"name": ["Mrs. Warm Up"], "country": ["United States"]},
        }
    )
    fake_company = Entity.from_dict(
        {
            "schema": "Company",
            "id": "warm-up-company",
            "properties": {
                "name": ["Warm-Up Company"],
                # Using the country name instead of the ISO code to trigger loading the country names data
                "country": ["Russia"],
            },
        }
    )
    for entity in [fake_person, fake_company]:
        for algo in ENABLED_ALGORITHMS:
            config = algo.default_config()
            algo.compare(entity, entity, config)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await warm_up()
    log.info(
        "Setting up background refresh",
        crontab=settings.CRONTAB,
        auto_reindex=settings.AUTO_REINDEX,
    )
    settings.CRON = aiocron.crontab(settings.CRONTAB, func=cron_task)
    yield
    await close_provider()


async def json_exception_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    try:
        response = await call_next(request)
    except Exception as exc:
        log.exception("Exception during request: %s" % type(exc))
        response = JSONResponse(status_code=500, content={"status": "error"})
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
    app.middleware("http")(json_exception_middleware)
    app.add_middleware(RequestLogMiddleware)
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
