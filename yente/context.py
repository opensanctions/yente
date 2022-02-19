from optparse import Option
from typing import Any, Dict, Optional
from uuid import uuid4
from fastapi import Request
from contextvars import ContextVar

contexts = ContextVar[Optional["Context"]]("reqctx", default=None)


class Context(object):
    def __init__(self, user_id: Optional[str]):
        self.user_id = user_id
        self.trace_id = uuid4().hex

    @property
    def log(self) -> Dict[str, Any]:
        return dict(user_id=self.user_id, trace_id=self.trace_id)

    @classmethod
    def from_request(cls, request: Request) -> "Context":
        user_id = request.headers.get("authorization")
        if user_id is not None and " " in user_id:
            _, user_id = user_id.split(" ", 1)
        obj = cls(user_id)
        contexts.set(obj)
        return obj

    @classmethod
    def get(cls) -> "Context":
        obj = contexts.get()
        if obj is None:
            obj = cls(None)
        return obj
