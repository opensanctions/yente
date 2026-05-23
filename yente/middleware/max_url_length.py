from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


class MaxURLLengthMiddleware:
    """Reject requests whose URL exceeds ``settings.MAX_URL_LENGTH`` with HTTP 414.

    Defends against an upstream parser bug (``httptools.parse_url`` wraps at
    65 536 bytes and silently truncates the path/query) by capping URL length
    below the wrap point so a client error is returned instead of data loss.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            raw_path = scope.get("raw_path") or scope["path"].encode()
            query_string = scope.get("query_string", b"")
            url_len = len(raw_path) + len(query_string) + (1 if query_string else 0)
            if url_len > settings.MAX_URL_LENGTH:
                log.warn(
                    "Request URI too long",
                    url_length=url_len,
                    limit=settings.MAX_URL_LENGTH,
                    path=scope["path"][:200],
                )
                response = JSONResponse(
                    status_code=414,
                    content={"detail": "Request URI too long"},
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
