from yente.data.util import iso_to_version


def test_iso_to_version_basic() -> None:
    assert iso_to_version("2024-05-15T12:34:56") == "20240515123456"


def test_iso_to_version_with_offset() -> None:
    assert iso_to_version("2024-05-15T12:34:56+00:00") == "20240515123456"


def test_iso_to_version_empty() -> None:
    assert iso_to_version("") is None
