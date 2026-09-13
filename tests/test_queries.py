import itertools
import string
from typing import Any
from unittest import mock

from yente.data.entity import Entity
from yente.search.queries import (
    FUZZY_BOOST,
    JOINED_BOOST,
    MAX_PARTS,
    MAX_SYMBOLS_PER_PART,
    SYMBOL_BOOST,
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
    with mock.patch("yente.settings.MATCH_FUZZY", True):
        parts = part_clauses(names_query(entity))
    fuzzy = channel(parts["putin"], "constant_score")
    assert fuzzy == {
        "constant_score": {
            "filter": {
                "fuzzy": {
                    "name_parts": {
                        "value": "putin",
                        "fuzziness": "AUTO",
                        "prefix_length": 1,
                        "max_expansions": 200,
                    }
                }
            },
            "boost": FUZZY_BOOST,
        }
    }


def test_fuzzy_channel_off():
    entity = make_entity("q-putin-nofuzzy", "Person", {"name": ["Vladimir Putin"]})
    with mock.patch("yente.settings.MATCH_FUZZY", False):
        parts = part_clauses(names_query(entity))
    assert set(parts) == {"vladimir", "putin"}
    for channels in parts.values():
        assert channel(channels, "constant_score") is None
        assert channel(channels, "term") is not None


def test_fuzzy_channel_skips_short_parts():
    entity = make_entity("q-li", "Person", {"name": ["Li Na"]})
    with mock.patch("yente.settings.MATCH_FUZZY", True):
        parts = part_clauses(names_query(entity))
    assert set(parts) == {"li", "na"}
    for channels in parts.values():
        assert channel(channels, "constant_score") is None


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
    joined = [c for c in shoulds if "terms" in c]
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
