from datetime import datetime
import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from typing import Any, Callable, Awaitable, Optional

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
        start_time = datetime.now()

        http_request_info: dict[str, Any] = {
            "requestMethod": request.method,
            "requestUrl": str(request.url),
            "userAgent": request.headers.get("User-Agent"),
        }
        if client_ip := get_client_ip(request):
            http_request_info["remoteIp"] = client_ip
        if referer := request.headers.get("referer"):
            http_request_info["referer"] = referer

        previous_contextvars = structlog.contextvars.bind_contextvars(
            # This field is special in Google Cloud Logging, see
            # https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#HttpRequest
            httpRequest=http_request_info,
            # Not part of the Google-spec'd httpRequest object, but we want to capture the request path for easier filtering.
            requestPath=request.url.path,
        )

        response = await call_next(request)

        http_request_info["status"] = response.status_code
        # Google Cloud LogEntry.HttpRequest specifies this is as a string like "0.5s",
        # but that doesn't actually work - who knows why.
        latency = datetime.now() - start_time
        http_request_info["latency"] = {
            "seconds": int(latency.total_seconds()),
            "nanos": int(latency.microseconds * 1000),
        }

        log.info(
            f"{request.method} {request.url.path}",
            logType="request_completed",
            httpRequest=http_request_info,
        )

        structlog.contextvars.reset_contextvars(**previous_contextvars)
        return response
