import pytest
from hashlib import sha1
from pathlib import Path
from typing import Any, List

from yente import settings
from yente.data.loader import load_json_lines, split_json_lines
from yente.exc import ChecksumError


async def _aiter(chunks: List[bytes]):
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_split_json_lines_record_per_chunk() -> None:
    chunks = [b'{"a":1}\n', b'{"b":2}\n']
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == [{"a": 1}, {"b": 2}]


@pytest.mark.asyncio
async def test_split_json_lines_multiple_records_in_one_chunk() -> None:
    chunks = [b'{"a":1}\n{"b":2}\n{"c":3}\n']
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == [{"a": 1}, {"b": 2}, {"c": 3}]


@pytest.mark.asyncio
async def test_split_json_lines_record_split_across_chunks() -> None:
    chunks = [b'{"a":', b"1", b"}\n"]
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == [{"a": 1}]


@pytest.mark.asyncio
async def test_split_json_lines_trailing_record_without_newline() -> None:
    chunks = [b'{"a":1}\n{"b":2}']
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == [{"a": 1}, {"b": 2}]


@pytest.mark.asyncio
async def test_split_json_lines_skips_empty_lines() -> None:
    chunks = [b'\n\n{"a":1}\n\n{"b":2}\n\n']
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == [{"a": 1}, {"b": 2}]


@pytest.mark.asyncio
async def test_split_json_lines_empty_input() -> None:
    chunks: List[bytes] = []
    out = [x async for x in split_json_lines(_aiter(chunks))]
    assert out == []


@pytest.mark.asyncio
async def test_load_json_lines_file_branch_matching_checksum(
    httpx_mock: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(settings, "STREAM_LOAD", False)
    monkeypatch.setattr(settings, "DATA_PATH", tmp_path)

    url = "https://example.com/entities.ftm.json"
    content = b'{"a":1}\n{"b":2}\n'
    httpx_mock.add_response(200, url=url, content=content)

    lines = [
        x
        async for x in load_json_lines(
            url, "test-match", expected_checksum=sha1(content).hexdigest()
        )
    ]
    assert lines == [{"a": 1}, {"b": 2}]
    assert not (tmp_path / "test-match").exists()


@pytest.mark.asyncio
async def test_load_json_lines_file_branch_mismatched_checksum(
    httpx_mock: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(settings, "STREAM_LOAD", False)
    monkeypatch.setattr(settings, "DATA_PATH", tmp_path)

    url = "https://example.com/entities.ftm.json"
    content = b'{"a":1}\n{"b":2}\n'
    httpx_mock.add_response(200, url=url, content=content)

    with pytest.raises(ChecksumError):
        [
            x
            async for x in load_json_lines(
                url, "test-mismatch", expected_checksum="0" * 40
            )
        ]
    assert not (tmp_path / "test-mismatch").exists()
