import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from .conftest import client

_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture(autouse=True)
def span_exporter():
    """Provide the in-memory span exporter, clearing spans between tests."""
    _exporter.clear()
    yield _exporter


def test_fastapi_instrumentation_creates_spans(span_exporter):
    """Verify that HTTP requests produce OTEL spans."""
    response = client.get("/healthz")
    assert response.status_code == 200

    spans = span_exporter.get_finished_spans()
    http_spans = [s for s in spans if "healthz" in s.name or "GET" in s.name]
    assert len(http_spans) >= 1, f"Expected HTTP span, got: {[s.name for s in spans]}"


@pytest.mark.asyncio
async def test_search_provider_creates_spans(search_provider, span_exporter):
    """Verify that SearchProvider operations produce OTEL spans."""
    from yente import settings

    result = await search_provider.search(
        index=settings.ENTITY_INDEX,
        query={"match_all": {}},
        size=1,
    )
    assert result is not None

    spans = span_exporter.get_finished_spans()
    search_spans = [s for s in spans if s.name == "SearchProvider.search"]
    assert (
        len(search_spans) == 1
    ), f"Expected 1 search span, got: {[s.name for s in spans]}"

    span = search_spans[0]
    assert span.attributes["db.system.name"] in ("elasticsearch", "opensearch")
    assert span.attributes["db.operation.name"] == "search"


@pytest.mark.asyncio
async def test_search_provider_records_errors_in_spans(search_provider, span_exporter):
    """Verify that SearchProvider errors are recorded in OTEL spans."""
    from yente.exc import YenteNotFoundError

    fake_index = "nonexistent-index-otel-test"
    with pytest.raises((YenteNotFoundError, Exception)):
        await search_provider.refresh(fake_index)

    spans = span_exporter.get_finished_spans()
    error_spans = [s for s in spans if s.status.status_code == trace.StatusCode.ERROR]
    assert len(error_spans) >= 1, f"Expected error span, got: {[s.name for s in spans]}"
