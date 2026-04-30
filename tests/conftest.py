# mypy: ignore-errors
import re
import pytest
import pytest_asyncio
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4
from pathlib import Path
from unittest.mock import Mock, patch
from contextlib import contextmanager
from fastapi.testclient import TestClient
from followthemoney import model
from followthemoney.dataset.util import dataset_name_check
import orjson

from yente import settings
from yente.app import create_app
from yente.search.indexer import update_index
from yente.provider import with_provider, close_provider


run_id = uuid4().hex
settings.TESTING = True
FIXTURES_PATH = Path(__file__).parent / "fixtures"
VERSIONS_PATH = FIXTURES_PATH / "versions.json"
MANIFEST_PATH = FIXTURES_PATH / "manifest.yml"
settings.MANIFEST = str(MANIFEST_PATH)
settings.INDEX_NAME = f"yente-test-{run_id}"
settings.ENTITY_INDEX = f"{settings.INDEX_NAME}-entities"
settings.AUTO_REINDEX = False

app = create_app()
client = TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def manifest():
    settings.MANIFEST = str(MANIFEST_PATH)


@pytest.fixture(scope="function", autouse=False)
def sanctions_catalog():
    manifest_tmp = settings.MANIFEST
    settings.MANIFEST = str(FIXTURES_PATH / "sanctions.yml")
    yield
    settings.MANIFEST = manifest_tmp


@pytest_asyncio.fixture(scope="function", autouse=False)
async def search_provider():
    async with with_provider() as provider:
        yield provider


@pytest_asyncio.fixture(scope="session", autouse=True)
async def load_data():
    await update_index(force=True)
    yield

    # Clean up the indices created by this test run
    async with with_provider() as provider:
        for index in await provider.get_all_indices():
            if index.startswith(settings.INDEX_NAME):
                await provider.delete_index(index)


@pytest_asyncio.fixture(autouse=True)
async def flush_cached_es():
    """Reference: https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154"""
    try:
        yield
    finally:
        await close_provider()


# Wire format for first_seen / last_seen / last_change on API responses.
# 19 chars, ISO 8601, `T` separator, seconds precision, no fractional
# seconds and no timezone suffix — e.g. "2023-04-20T18:00:25". Kept tight
# because changing how Pydantic models build (validate vs. construct) can
# silently shift this format and downstream consumers depend on it.
ISO_SECONDS_NO_TZ = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")


def assert_iso_seconds_no_tz(value: str) -> None:
    assert isinstance(value, str), (type(value), value)
    assert ISO_SECONDS_NO_TZ.match(value), value
    # Belt-and-braces: it should parse as a naive datetime and round-trip.
    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is None, value
    assert parsed.microsecond == 0, value
    assert parsed.isoformat() == value, value


# Minimum wire shape of an EntityResponse. Responses are built via
# model_construct, which skips pydantic validation — so any drift in the
# ES payload (renamed keys, type changes) would pass straight through to
# clients unnoticed. This set pins what clients are guaranteed to see;
# subclasses like ScoredEntityResponse add more keys on top.
REQUIRED_ENTITY_KEYS = {
    "id",
    "caption",
    "schema",
    "properties",
    "datasets",
    "referents",
    "target",
    "first_seen",
    "last_seen",
    "last_change",
}


def assert_entity_shape(data: Dict[str, Any]) -> None:
    missing = REQUIRED_ENTITY_KEYS - data.keys()
    assert not missing, (missing, sorted(data.keys()))
    assert isinstance(data["id"], str) and data["id"], data["id"]
    assert isinstance(data["caption"], str) and data["caption"], data["caption"]
    assert data["schema"] in model.schemata, data["schema"]
    assert isinstance(data["properties"], dict), data["properties"]
    for prop, values in data["properties"].items():
        assert isinstance(prop, str), prop
        assert isinstance(values, list), (prop, values)
        for value in values:
            # Nested entity references appear as dicts in nested=true responses
            # and must themselves be well-shaped EntityResponses.
            if isinstance(value, dict):
                assert_entity_shape(value)
            else:
                assert isinstance(value, str), (prop, value)
    assert isinstance(data["datasets"], list), data["datasets"]
    for name in data["datasets"]:
        assert isinstance(name, str), name
        dataset_name_check(name)
    assert isinstance(data["referents"], list), data["referents"]
    assert all(isinstance(r, str) for r in data["referents"]), data["referents"]
    assert isinstance(data["target"], bool), data["target"]
    for key in ("first_seen", "last_seen", "last_change"):
        if data[key] is not None:
            assert_iso_seconds_no_tz(data[key])


@contextmanager
def patch_catalog_response(response_data: dict):
    """Context manager to patch AsyncClient for catalog responses."""
    mock_response = Mock()
    mock_response.content = orjson.dumps(response_data)

    with patch(
        "yente.data.util.httpx.AsyncClient", autospec=True
    ) as mock_client_constructor:
        mock_client = mock_client_constructor.return_value.__aenter__.return_value
        mock_client.get.return_value = mock_response
        yield mock_client, mock_client_constructor
