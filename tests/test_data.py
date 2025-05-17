import pytest
from pathlib import Path
from followthemoney import model

from yente.data import get_catalog
from yente.data.loader import load_json_lines
from yente.data.util import get_url_local_path
from yente.data.util import (
    phonetic_names,
    expand_dates,
    index_name_parts,
    index_name_keys,
)


@pytest.mark.asyncio
async def test_manifest():
    catalog = await get_catalog()
    assert len(catalog.datasets), catalog.datasets


@pytest.mark.asyncio
async def test_local_dataset():
    catalog = await get_catalog()
    ds = catalog.require("parteispenden")
    assert ds.load
    assert ds.entities_url is not None
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
    person = model.get("Person")
    assert person is not None
    phonemes = phonetic_names(person, ["Vladimir Putin"])
    assert len(phonemes) == 2
    assert "PTN" in phonemes
    phonemes = phonetic_names(person, ["Влади́мир Влади́мирович ПУ́ТИН"])
    assert len(phonemes) == 3
    assert "PTN" in phonemes
    shortened = phonetic_names(person, ["Влади́мир В. ПУ́ТИН"])
    assert len(shortened) == 2
    phonemes = phonetic_names(person, ["Vladimir Peter Putin"])
    assert len(phonemes) == 3
    company = model.get("Company")
    assert company is not None
    phonemes = phonetic_names(company, ["OAO Gazprom"])
    assert len(phonemes) == 1


def test_expand_dates():
    dates = ["2023-01-01"]
    expanded = expand_dates(dates)
    assert len(expanded) == 3
    assert "2023" in expanded
    assert "2023-01" in expanded
    assert "2023-01-01" in expanded
    dates = ["2023-01-01", "2022"]
    expanded = expand_dates(dates)
    assert len(expanded) == 4
    assert "2022" in expanded


def test_index_name_parts():
    person = model.get("Person")
    assert person is not None
    parts = index_name_parts(person, ["Vladimir Putin"])
    assert len(parts) == 2
    assert "vladimir" in parts
    assert "putin" in parts
    parts = index_name_parts(person, ["Влади́мир В. ПУ́ТИН"])
    assert len(parts) == 4
    assert "владимир" in parts
    assert "путин" in parts
    assert "vladimir" in parts


def test_index_name_keys():
    person = model.get("Person")
    assert person is not None
    keys = index_name_keys(person, ["Vladimir Putin"])
    assert len(keys) == 1
    assert "putinvladimir" in keys
    keys = index_name_keys(person, ["Влади́мир ПУ́ТИН"])
    assert len(keys) == 1
    assert "putinvladimir" in keys
