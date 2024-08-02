from .conftest import client
import re


def test_trace_context() -> None:
    # Works when receiving a valid trace context
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "tracestate": "rojo=00f067aa0ba902b7",
    }
    parent_pat = re.compile(r"00-0af7651916cd43dd8448eb211c80319c-[0-9a-f]{16}-01")
    state_pat = re.compile(r"yente=[0-9a-f]{16},\s?rojo=00f067aa0ba902b7")
    res = client.get("/search/default?q=vladimir putin", headers=headers)
    assert "traceparent" in res.headers
    assert "tracestate" in res.headers
    assert parent_pat.match(res.headers["traceparent"])
    assert state_pat.match(res.headers["tracestate"])
    # Works when not receiving a trace context
    res = client.get("/search/default?q=vladimir putin")
    assert "traceparent" in res.headers
    assert "tracestate" in res.headers
    assert re.match(
        r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", res.headers["traceparent"]
    )
    assert re.match(r"yente=[0-9a-f]{16}", res.headers["tracestate"])
    # Works with a broken trace context
    headers = {
        "traceparent": "ff-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
        "tracestate": "rojo=00f067aa0ba902b7",
    }
    res = client.get("/search/default?q=vladimir putin")
    assert "traceparent" in res.headers
    assert "tracestate" in res.headers
    assert re.match(
        r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", res.headers["traceparent"]
    )
    assert re.match(r"yente=[0-9a-f]{16}", res.headers["tracestate"])
