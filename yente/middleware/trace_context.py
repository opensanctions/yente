from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from typing import Any, Tuple, List
import secrets
from structlog.contextvars import get_contextvars, bind_contextvars

VENDOR_CODE = (
    "yente"  # It's available! https://w3c.github.io/tracestate-ids-registry/#registry
)


class TraceParent:
    __slots__ = ["version", "trace_id", "parent_id", "trace_flags"]

    def __init__(self, version: str, trace_id: str, parent_id: str, trace_flags: str):
        self.version = version
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.trace_flags = trace_flags

    def __str__(self) -> str:
        return f"{self.version}-{self.trace_id}-{self.parent_id}-{self.trace_flags}"

    @classmethod
    def create(cls) -> "TraceParent":
        return cls("00", secrets.token_hex(16), secrets.token_hex(8), "00")

    @classmethod
    def from_str(cls, traceparent: str | None) -> "TraceParent":
        """
        Parse a traceparent header string into a TraceParent object created with a new parent_id.
        """
        if traceparent is None:
            return cls.create()
        parts = traceparent.split("-")
        try:
            version, trace_id, parent_id, trace_flags = parts[:4]
        except Exception:
            raise ValueError(f"Invalid traceparent: {traceparent}")
        if int(version, 16) == 255:
            raise ValueError(f"Unsupported version: {version}")
        for i in trace_id:
            if i != "0":
                break
        else:
            raise ValueError(f"Invalid trace_id: {trace_id}")
        for i in parent_id:
            if i != "0":
                break
        else:
            raise ValueError(f"Invalid parent_id: {parent_id}")

        return cls(version, trace_id, secrets.token_hex(8), trace_flags)


class TraceState:
    __slots__ = ["tracestate"]

    def __init__(self, tracestate: List[Tuple[str, str]] = []):
        self.tracestate = tracestate

    @classmethod
    def create(cls, parent: TraceParent, prev_state: str = "") -> "TraceState":
        spans_out: List[Tuple[str, str]] = []
        for span in prev_state.split(","):
            parts = span.split("=")
            if len(parts) != 2:
                # We are allowed to discard invalid states
                continue
            vendor, value = parts
            if vendor == VENDOR_CODE:
                continue
            spans_out.append((vendor.lower().strip(), value.lower().strip()))
        spans_out.insert(0, (VENDOR_CODE, f"{parent.parent_id}"))
        return cls(spans_out)

    def __str__(self) -> str:
        return ",".join([f"{k}={v}" for k, v in self.tracestate])


class TraceContext:
    __slots__ = ["traceparent", "tracestate"]

    def __init__(self, traceparent: TraceParent, tracestate: TraceState):
        self.traceparent = traceparent
        self.tracestate = tracestate

    def __repr__(self) -> str:
        return str(
            {
                "traceparent": str(self.traceparent),
                "tracestate": str(self.tracestate),
            }
        )


def get_trace_context() -> TraceContext | None:
    vars = get_contextvars()
    if "trace_context" in vars:
        trace_context = vars["trace_context"]
        if isinstance(trace_context, TraceContext):
            return trace_context
    return None


class TraceContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Any) -> Any:
        parent_header = request.headers.get("traceparent")
        try:
            traceparent = TraceParent.from_str(parent_header)
        except Exception:
            traceparent = TraceParent.create()
        state = request.headers.get("tracestate", "")
        try:
            tracestate = TraceState.create(traceparent, state)
        except Exception:
            tracestate = TraceState.create(traceparent, "")
        context = TraceContext(traceparent, tracestate)
        bind_contextvars(trace_context=context)
        resp = await call_next(request)
        resp.headers["traceparent"] = str(traceparent)
        resp.headers["tracestate"] = str(tracestate)
        return resp
