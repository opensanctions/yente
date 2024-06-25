# mypy: ignore-errors
import pytest
import pytest_asyncio
from uuid import uuid4
from pathlib import Path
from fastapi.testclient import TestClient
import httpx

from yente import settings
from yente.app import create_app
from yente.search.base import ESSearchProvider


run_id = uuid4().hex
settings.TESTING = True
FIXTURES_PATH = Path(__file__).parent.parent / "../fixtures/"
VERSIONS_PATH = FIXTURES_PATH / "versions.json"
MANIFEST_PATH = FIXTURES_PATH / "manifest.yml"
settings.MANIFEST = str(MANIFEST_PATH)
settings.UPDATE_TOKEN = "test"
settings.ES_INDEX = f"yente-test-{run_id}"
settings.ENTITY_INDEX = f"{settings.ES_INDEX}-entities"
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
    provider = await ESSearchProvider.create()
    yield provider


def clear_state():
    pass


@pytest_asyncio.fixture(scope="function", autouse=True)
async def clean_es():
    provider = await ESSearchProvider.create()
    await provider.delete_index("*")
    yield
    await provider.delete_index("*")


@pytest.fixture(autouse=True)
def flush_cached_es():
    """Reference: https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154"""
    try:
        yield
    finally:
        clear_state()
