import pytest
import asyncio
from uuid import uuid4

from yente import settings
from yente.index import get_es


run_id = uuid4().hex
settings.TESTING = True
# settings.SCOPE_DATASET = "eu_meps"
settings.DATA_INDEX = "file:////Users/pudo/Code/yente/tests/fixtures/index.json"
settings.ES_INDEX = f"yente-test-{run_id}"
settings.ENTITY_INDEX = f"{settings.ES_INDEX}-entities"
settings.STATEMENT_INDEX = f"{settings.ES_INDEX}-statements"


@pytest.fixture(autouse=True)
def flush_cached_es():
    """Reference: https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154"""
    yield
    get_es.cache_clear()
