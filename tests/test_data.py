import pytest
from .conftest import client

from yente.data import get_datasets, get_manifest


@pytest.mark.asyncio
async def test_manifest():
    manifest = await get_manifest()
    assert len(manifest.datasets), manifest.datasets
    assert manifest.schedule is None


@pytest.mark.asyncio
async def test_local_dataset():
    datasets = await get_datasets()
    ds = datasets["parteispenden"]
    entities = [e async for e in ds.entities()]
    assert len(entities) > 10, entities
