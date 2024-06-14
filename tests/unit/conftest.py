import pytest
import pytest_asyncio
from uuid import uuid4
from pathlib import Path
from fastapi.testclient import TestClient

from yente import settings
from yente.app import create_app
from yente.search.base import ESSearchProvider


run_id = uuid4().hex
settings.TESTING = True
FIXTURES_PATH = Path(__file__).parent / "../fixtures/"
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
    settings.MANIFEST = str(FIXTURES_PATH / "sanctions.yml")


@pytest_asyncio.fixture(scope="function", autouse=False)
async def search_provider():
    provider = await ESSearchProvider.create()
    yield provider
    await provider.delete_index("test*")


def clear_state():
    pass


@pytest.fixture(scope="session", autouse=False)
def load_data():
    client.post(f"/updatez?token={settings.UPDATE_TOKEN}&sync=true")
    clear_state()
    yield


@pytest.fixture(autouse=True)
def flush_cached_es():
    """Reference: https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154"""
    try:
        yield
    finally:
        clear_state()
