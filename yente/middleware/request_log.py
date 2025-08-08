import time
import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from typing import Callable, Awaitable, Optional

from yente.logs import get_logger


log = get_logger(__name__)


def get_client_ip(request: Request) -> Optional[str]:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
        return client_ip

    return request.client.host if request.client else None


class RequestLogMiddleware(BaseHTTPMiddleware):
    """Middleware to set some log context based on the HTTP request."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start_time = time.time()

        http_request_info = {
            "requestMethod": request.method,
            "requestUrl": str(request.url),
            "userAgent": request.headers.get("User-Agent"),
            "referer": request.headers.get("referer"),
        }
        if client_ip := get_client_ip(request):
            http_request_info["remoteIp"] = client_ip

        previous_contextvars = structlog.contextvars.bind_contextvars(
            # This field is special in Google Cloud Logging, see
            # https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#HttpRequest
            httpRequest=http_request_info,
            # Not part of the Google-spec'd httpRequest object, but we want to capture the request path for easier filtering.
            requestPath=request.url.path,
        )

        response = await call_next(request)

        http_request_info["status"] = str(response.status_code)
        http_request_info["took"] = f"{(time.time() - start_time)}s"

        log.info(
            f"{request.method} {request.url.path}",
            logType="request_completed",
            httpRequest=http_request_info,
        )

        structlog.contextvars.reset_contextvars(**previous_contextvars)
        return response
