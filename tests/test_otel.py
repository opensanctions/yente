import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture(autouse=True)
def span_exporter():
    """Provide the in-memory span exporter, clearing spans between tests."""
    _exporter.clear()
    yield _exporter


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
    spans = [s for s in spans if s.name == "search"]
    assert len(spans) == 1, f"Expected 1 search span, got: {[s.name for s in spans]}"

    span = spans[0]
    assert span.attributes["db.system"] in ("elasticsearch", "opensearch")
    assert span.attributes["db.operation"] == "search"


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
