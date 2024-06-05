import pytest
from pathlib import Path

from yente.data import get_catalog
from yente.data.loader import load_json_lines
from yente.data.util import get_url_local_path
from yente.data.util import phonetic_names


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


def test_get_url_local_path():
    out = get_url_local_path("http://banana.com/bla.txt")
    assert out is None
    out = get_url_local_path("https://banana.com/bla.txt")
    assert out is None
    out = get_url_local_path("file:///etc/passwd")
    assert isinstance(out, Path)
    assert "/etc/passwd" in out.as_posix()
    out = get_url_local_path("/etc/passwd")
    assert isinstance(out, Path)
    assert "/etc/passwd" in out.as_posix()
    out = get_url_local_path(__file__)
    assert isinstance(out, Path)

    with pytest.raises(RuntimeError):
        get_url_local_path("/no/such/path.csv")


def test_phonetic_names():
    phonemes = phonetic_names(["Vladimir Putin"])
    assert len(phonemes) == 2
    assert "PTN" in phonemes
    phonemes = phonetic_names(["Влади́мир Влади́мирович ПУ́ТИН"])
    assert len(phonemes) == 3
    assert "PTN" in phonemes
    shortened = phonetic_names(["Влади́мир В. ПУ́ТИН"])
    assert len(shortened) == 2
    phonemes = phonetic_names(["Vladimir Peter Putin"])
    assert len(phonemes) == 3
    phonemes = phonetic_names(["OAO Gazprom"])
    assert len(phonemes) == 1
