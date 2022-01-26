import pytest
import asyncio
import warnings
from uuid import uuid4

from fastapi.testclient import TestClient


from yente import settings
from yente.index import get_es
from yente.app import app


run_id = uuid4().hex
settings.TESTING = True
settings.SCOPE_DATASET = "wd_curated"
settings.UPDATE_TOKEN = "test"
settings.ES_INDEX = f"yente-test-{run_id}"
settings.ENTITY_INDEX = f"{settings.ES_INDEX}-entities"
settings.STATEMENT_INDEX = f"{settings.ES_INDEX}-statements"
settings.STATEMENT_API = False

client = TestClient(app)


def clear_state():
    async def shutdown():
        es = await get_es()
        await es.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(shutdown())
    get_es.cache_clear()


@pytest.fixture(scope="session", autouse=True)
def load_data():
    client.post(f"/updatez?token={settings.UPDATE_TOKEN}&sync=true")
    clear_state()
    yield


@pytest.fixture(autouse=True)
def flush_cached_es():
    """Reference: https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154"""
    yield
    clear_state()
