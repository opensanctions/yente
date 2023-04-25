import pytest
from pathlib import Path

from yente.data import get_catalog
from yente.data.loader import load_json_lines
from yente.data.util import resolve_url_type
from yente.data.util import soundex_names


@pytest.mark.asyncio
async def test_manifest():
    catalog = await get_catalog()
    assert len(catalog.datasets), catalog.datasets


@pytest.mark.asyncio
async def test_local_dataset():
    catalog = await get_catalog()
    ds = catalog.require("parteispenden")
    assert ds.load
    assert "donations.ijson" in ds.entities_url
    lines = list()
    async for line in load_json_lines(ds.entities_url, "test"):
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


def test_soundex_names():
    soundexes = soundex_names(['Vladimir Putin'])
    assert len(soundexes) == 2
    assert 'P350' in soundexes
    assert 'V435' in soundexes
    soundexes = soundex_names(['Влади́мир Влади́мирович ПУ́ТИН'])
    assert len(soundexes) == 2
    assert 'P350' in soundexes
    assert 'V435' in soundexes
    shortened = soundex_names(['Влади́мир В. ПУ́ТИН'])
    assert len(shortened) == 2
    soundexes = soundex_names(['Vladimir Peter Putin'])
    assert len(soundexes) == 3
