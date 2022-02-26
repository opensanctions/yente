import time
import structlog
from uuid import uuid4
from normality import slugify
from typing import cast
from fastapi import FastAPI
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from structlog.contextvars import clear_contextvars, bind_contextvars

from yente import settings
from yente.routers import reconcile, search, statements, admin

log: structlog.stdlib.BoundLogger = structlog.get_logger("yente")
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


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    start_time = time.time()
    user_id = request.headers.get("authorization")
    if user_id is not None:
        if " " in user_id:
            _, user_id = user_id.split(" ", 1)
        user_id = slugify(user_id)
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
