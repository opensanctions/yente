import pytest

from yente.data.util import iso_to_version, name_part_variants


def test_iso_to_version_basic() -> None:
    assert iso_to_version("2024-05-15T12:34:56") == "20240515123456"


def test_iso_to_version_with_offset() -> None:
    assert iso_to_version("2024-05-15T12:34:56+00:00") == "20240515123456"


def test_iso_to_version_empty() -> None:
    assert iso_to_version("") is None


@pytest.mark.parametrize(
    "part,count",
    [
        ("a", 1),
        ("li", 1),
        ("kim", 4),
        ("jong", 5),
        ("putin", 6),
        # 1 + n + n(n-1)/2 for n = 6 and n = 9
        ("valery", 22),
        ("rotenberg", 46),
    ],
)
def test_name_part_variants_count(part: str, count: int) -> None:
    variants = name_part_variants(part)
    assert part in variants
    assert len(variants) == count


def test_name_part_variants_short_parts_are_themselves() -> None:
    assert name_part_variants("v") == {"v"}
    assert name_part_variants("li") == {"li"}


@pytest.mark.parametrize("length,count", [(64, 2081), (65, 1), (384, 1)])
def test_name_part_variants_long_parts(length: int, count: int) -> None:
    part = "".join(chr(0x4E00 + i) for i in range(length))
    variants = name_part_variants(part)
    assert part in variants
    assert len(variants) == count


def test_name_part_variants_one_deletion_band() -> None:
    assert name_part_variants("kim") == {"kim", "im", "km", "ki"}
    # Repeated letters collapse into the same variant.
    assert name_part_variants("anna") == {"anna", "nna", "ana", "ann"}


@pytest.mark.parametrize(
    "a,b",
    [
        # substitution, insertion, deletion, adjacent transposition at k = 1
        ("putin", "pusin"),
        ("putin", "puutin"),
        ("putin", "ptin"),
        ("putin", "putni"),
        # the same four edits at the first letter
        ("putin", "butin"),
        ("putin", "vputin"),
        ("putin", "utin"),
        ("putin", "uptin"),
        # two edits at k = 2
        ("ermakov", "ermakoff"),
        ("zakharov", "zaharow"),
        ("alexander", "zlexandr"),
    ],
)
def test_name_part_variants_bridge_edits(a: str, b: str) -> None:
    assert name_part_variants(a) & name_part_variants(b)


@pytest.mark.parametrize(
    "a,b",
    [
        ("mohammed", "muhammad"),
        ("mohammad", "muhammed"),
        ("alexander", "aleksandr"),
        ("wagner", "vagner"),
        ("catherine", "katherine"),
        ("putin", "poutine"),
        ("yusuf", "yousef"),
        ("abdul", "abdool"),
        ("gaddafi", "qadhafi"),
        ("hezbollah", "hizballah"),
        ("ermakov", "ermacov"),
        ("zakharov", "zacharov"),
        ("sergey", "sergei"),
        ("dmitry", "dmitriy"),
        ("yevgeny", "evgeny"),
        ("nikolay", "nikolai"),
        ("mikhail", "michail"),
        ("khan", "han"),
        ("kim", "gim"),
        ("john", "jon"),
        ("jonathan", "johnathan"),
        ("stephen", "steven"),
        ("philip", "phillip"),
        ("isabel", "isabell"),
        ("nasser", "naser"),
        ("hussein", "hussain"),
        ("mahmoud", "mahmud"),
        ("ibrahim", "ibraheem"),
        ("rotenberg", "rottenberg"),
    ],
)
def test_name_part_variants_bridge_spelling_variants(a: str, b: str) -> None:
    assert name_part_variants(a) & name_part_variants(b)


@pytest.mark.parametrize(
    "a,b",
    [
        ("john", "smith"),
        ("vladimir", "putin"),
        ("kim", "lee"),
        ("li", "lee"),
        ("maria", "petrov"),
    ],
)
def test_name_part_variants_do_not_bridge_unrelated(a: str, b: str) -> None:
    assert not name_part_variants(a) & name_part_variants(b)
