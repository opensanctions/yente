from pathlib import Path
import pytest

from .conftest import client

from yente.data import get_catalog, get_manifest
from yente.data.util import resolve_url_type


@pytest.mark.asyncio
async def test_manifest():
    manifest = await get_manifest()
    assert len(manifest.datasets), manifest.datasets
    assert manifest.schedule is None


@pytest.mark.asyncio
async def test_local_dataset():
    catalog = await get_catalog()
    ds = catalog.require("parteispenden")
    entities = [e async for e in ds.entities()]
    assert len(entities) > 10, entities


def test_resolve_url_type():
    out = resolve_url_type("http://banana.com/bla.txt")
    assert isinstance(out, str)
    out = resolve_url_type(__file__)
    assert isinstance(out, Path)

    with pytest.raises(RuntimeError):
        resolve_url_type("ftp://banana.com/bla.txt")

    with pytest.raises(RuntimeError):
        resolve_url_type("/no/such/path.csv")
