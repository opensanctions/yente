import itertools
import string
from typing import Any
from unittest import mock

from yente.data.entity import Entity
from yente.data.util import name_part_variants
from yente.search.queries import (
    JOINED_BOOST,
    MAX_PARTS,
    MAX_SYMBOLS_PER_PART,
    SYMBOL_BOOST,
    VARIANTS_BOOST,
    WEAK_ALIAS_BOOST,
    names_query,
)

MANY_NAMES = [
    "Alexander Vyacheslavovich ZAKHAROV",
    "Aleksandr Vyacheslavovich Zakharov",
    "Александр Вячеславович Захаров",
    "Александр ЗАХАРОВ",
    "Захаров Александр Вячеславович",
    "Zakharov Aleksandr Vyacheslavovich",
    "Aleksandr Vjačeslavovič Zacharov",
]


def make_entity(id_: str, schema: str, properties: dict[str, Any]) -> Entity:
    # The name analysis is cached on the entity ID, so every test entity needs
    # its own ID.
    return Entity.from_dict(
        {"id": id_, "schema": schema, "properties": properties, "datasets": ["test"]}
    )


def part_clauses(shoulds: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Map each queried name part to the channel clauses inside its dis_max."""
    parts: dict[str, list[dict[str, Any]]] = {}
    for clause in shoulds:
        if "dis_max" not in clause:
            continue
        channels = clause["dis_max"]["queries"]
        parts[channels[0]["term"]["name_parts"]["value"]] = channels
    return parts


def channel(channels: list[dict[str, Any]], kind: str) -> dict[str, Any] | None:
    for clause in channels:
        if kind in clause:
            return clause
    return None


def variants_channel(channels: list[dict[str, Any]]) -> dict[str, Any] | None:
    for clause in channels:
        if "terms" in clause and "name_part_variants" in clause["terms"]:
            return clause
    return None


def test_one_clause_per_unique_part():
    entity = make_entity("q-many", "Person", {"name": MANY_NAMES})
    shoulds = names_query(entity)
    parts = part_clauses(shoulds)
    assert set(parts) == {
        "alexander",
        "aleksandr",
        "vyacheslavovich",
        "vjaceslavovic",
        "vaceslavovic",
        "zakharov",
        "zaharov",
        "zacharov",
    }
    for channels in parts.values():
        assert channels[0]["term"]["name_parts"]["boost"] == 1.0
    dis_maxes = [c for c in shoulds if "dis_max" in c]
    assert len(dis_maxes) == len(parts)
    for clause in dis_maxes:
        assert clause["dis_max"]["tie_breaker"] == 0.0


def test_fuzzy_channel():
    entity = make_entity("q-putin", "Person", {"name": ["Vladimir Putin"]})
    parts = part_clauses(names_query(entity))
    variants = variants_channel(parts["putin"])
    assert variants == {
        "terms": {
            "name_part_variants": ["ptin", "puin", "puti", "putin", "putn", "utin"],
            "boost": VARIANTS_BOOST,
        }
    }
    assert channel(parts["putin"], "term") == {
        "term": {"name_parts": {"value": "putin", "boost": 1.0}}
    }
    variants = variants_channel(parts["vladimir"])
    assert variants is not None
    assert set(variants["terms"]["name_part_variants"]) == name_part_variants(
        "vladimir"
    )
    assert channel(parts["vladimir"], "fuzzy") is None
    assert channel(parts["vladimir"], "constant_score") is None


def test_fuzzy_channel_skips_short_parts():
    entity = make_entity("q-li", "Person", {"name": ["Li Na Kim"]})
    parts = part_clauses(names_query(entity))
    assert set(parts) == {"li", "na", "kim"}
    assert variants_channel(parts["li"]) is None
    assert variants_channel(parts["na"]) is None
    kim = variants_channel(parts["kim"])
    assert kim is not None
    assert kim["terms"]["name_part_variants"] == ["im", "ki", "kim", "km"]


def test_symbol_channel():
    entity = make_entity("q-putin-sym", "Person", {"name": ["Vladimir Putin"]})
    parts = part_clauses(names_query(entity))
    symbols = channel(parts["putin"], "dis_max")
    assert symbols is not None
    assert symbols["dis_max"]["boost"] == SYMBOL_BOOST
    terms = symbols["dis_max"]["queries"]
    assert {"term": {"name_symbols": {"value": "NAME:KHUYLO", "boost": 1.0}}} in terms


def test_symbol_channel_absent_for_untagged_part():
    entity = make_entity("q-xyzzy", "Person", {"name": ["Xyzzyq Plughz"]})
    parts = part_clauses(names_query(entity))
    for channels in parts.values():
        assert channel(channels, "dis_max") is None


def test_symbol_channel_cap():
    entity = make_entity("q-li-cap", "Person", {"name": ["Li Na"]})
    parts = part_clauses(names_query(entity))
    li_symbols = channel(parts["li"], "dis_max")
    assert li_symbols is not None
    assert len(li_symbols["dis_max"]["queries"]) == MAX_SYMBOLS_PER_PART

    with mock.patch("yente.search.queries.MAX_SYMBOLS_PER_PART", 2):
        parts = part_clauses(names_query(entity))
    li_symbols = channel(parts["li"], "dis_max")
    assert li_symbols is not None
    assert len(li_symbols["dis_max"]["queries"]) == 2


def test_joined_clause():
    entity = make_entity(
        "q-joined", "Person", {"name": ["Vladimir Putin"], "alias": ["Vova Putin"]}
    )
    shoulds = names_query(entity)
    joined = [c for c in shoulds if "terms" in c and "name_joined" in c["terms"]]
    assert joined == [
        {
            "terms": {
                "name_joined": ["vladimirputin", "vovaputin"],
                "boost": JOINED_BOOST,
            }
        }
    ]


def test_weak_alias_clause():
    entity = make_entity(
        "q-weak", "Person", {"name": ["Vladimir Putin"], "weakAlias": ["Vova"]}
    )
    shoulds = names_query(entity)
    assert {"term": {"name_parts": {"value": "vova", "boost": WEAK_ALIAS_BOOST}}} in (
        shoulds
    )
    assert "vova" not in part_clauses(shoulds)


def test_parts_cap():
    tokens = ["".join(t) for t in itertools.product(string.ascii_lowercase, repeat=3)]
    entity = make_entity("q-big", "Person", {"name": [" ".join(tokens[:150])]})
    shoulds = names_query(entity)
    parts = part_clauses(shoulds)
    assert len(parts) == MAX_PARTS
    assert len([c for c in shoulds if "dis_max" in c]) == MAX_PARTS


def test_primary_name_parts_come_first():
    entity = make_entity(
        "q-order",
        "Person",
        {"name": ["Zebediah Quill"], "alias": ["Aaron Abbott"]},
    )
    parts = list(part_clauses(names_query(entity)))
    assert parts[:2] == ["zebediah", "quill"]
