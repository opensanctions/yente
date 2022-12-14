from pathlib import Path
import pytest

from .conftest import client

from yente.data import get_catalog, get_manifest
from yente.data.loader import load_json_lines, get_url_path

# from yente.search.indexer import entity_docs_from_path
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
    assert ds.load
    assert "donations.ijson" in ds.entities_url
    url_path = await get_url_path(ds.entities_url, "test")
    lines = list()
    async for line in load_json_lines(url_path):
        lines.append(line)
    assert len(lines) > 10, lines


def test_resolve_url_type():
    out = resolve_url_type("http://banana.com/bla.txt")
    assert isinstance(out, str)
    out = resolve_url_type(__file__)
    assert isinstance(out, Path)

    with pytest.raises(RuntimeError):
        resolve_url_type("ftp://banana.com/bla.txt")

    with pytest.raises(RuntimeError):
        resolve_url_type("/no/such/path.csv")
