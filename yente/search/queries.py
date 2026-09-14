import enum
from collections import defaultdict
from collections.abc import Generator, Iterable, Sequence
from pprint import pprint  # noqa
from typing import Any

from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.types import registry

# Same name analysis as the scorer, so the LRU cache is shared between them:
from nomenklatura.matching.logic_v2.names.analysis import entity_names
from rigour.names import Symbol

from yente.data.dataset import Dataset
from yente.data.util import entity_weak_names, index_symbols, name_part_variants
from yente.logs import get_logger
from yente.search.mapping import (
    NAME_JOINED_FIELD,
    NAME_PART_FIELD,
    NAME_SYMBOLS_FIELD,
    NAME_VARIANTS_FIELD,
)

log = get_logger(__name__)
Clause = dict[str, Any]
FilterSpec = tuple[str, str | bool]
Filters = Sequence[FilterSpec]
Sort = str | dict[str, dict[str, str]]

DEFAULT_SORTS: list[Sort] = [
    {"_score": {"order": "desc"}},
    {"entity_id": {"order": "asc", "unmapped_type": "keyword"}},
]

# Boost factors for non-name property types in entity queries, reflecting their
# relative importance in the LogicV2 scoring algorithm. Identifiers are near-
# deterministic match signals (0.85-0.98 weight in LogicV2), countries are modestly
# informative. Dates sit at the level of a single name part: a year-only birth date
# is shared by over a thousand records, and at a higher boost those records would
# outrank an exact two-part name match and fill the candidate window on their own.
TYPE_BOOSTS = {
    registry.identifier: 8.0,
    registry.date: 1.0,
    registry.phone: 3.0,
    registry.email: 3.0,
    registry.country: 1.5,
}

# Name candidate retrieval reaches an indexed entity through four channels, each an
# explicit statement of how a query name part may differ from an indexed one. Both
# sides are analysed by the same rigour normaliser, so a term is retrievable iff both
# produced the same string; Elasticsearch does no analysis of its own on these fields.
#
#   exact   `term` on `name_parts`: the comparable form (casefolded, latinised where a
#           script allows it, diacritics and punctuation stripped) is identical.
#   fuzzy   `terms` on `name_part_variants`: the query part and the indexed part share
#           a deletion variant (see `name_part_variants`), which is the case whenever
#           they are within the part's Damerau-Levenshtein budget of 0 edits up to 2
#           chars, 1 edit at 3-5, 2 edits from 6, at any position including the first
#           letter. Covers typos and short transliteration variants (mohammed/muhammad,
#           wagner/vagner). Also admits some pairs up to twice the budget apart.
#   symbol  `term` on `name_symbols`: rigour tagged both parts with the same known-name
#           identity, nickname, org class or the like. The only bridge for scripts that
#           are not latinised (Arabic, Han) and for variants beyond two edits
#           (alexander/aleksandr).
#   joined  `terms` on `name_joined`: a query name with its spaces removed equals an
#           indexed name with its spaces removed (alqaeda / Al Qaeda).
#
# The three per-part channels are combined with `dis_max` so a part counts once even
# when several channels hit it. Exact and symbol hits are IDF-scored `term`s, so a rare
# part or identity outranks a common one. A `terms` hit is constant-scored by ES, so a
# fuzzy hit is worth VARIANTS_BOOST however many variants a document shares, about the
# exact score of a part shared by 25,000 records, so an exact hit on any but the most
# common tokens ("of", "ltd", "de") outranks an approximate one.
NAME_PART_BOOST = 1.0
VARIANTS_BOOST = 6.0
SYMBOL_BOOST = 0.9
JOINED_BOOST = 1.0
WEAK_ALIAS_BOOST = 0.9
# Clause budget: MAX_PARTS * (2 + MAX_SYMBOLS_PER_PART) + 2 stays under the ES default
# `indices.query.bool.max_clause_count` of 4096. Observed maxima on real queries are
# 22 unique parts and 29 symbols on one part, so the caps are safety, not tuning.
MAX_PARTS = 100
MAX_SYMBOLS_PER_PART = 30


class Operator(enum.StrEnum):
    AND = "AND"
    OR = "OR"


def tq(field: str, value: str | bool, boost: float = 1.0) -> Clause:
    return {"term": {field: {"value": value, "boost": boost}}}


def tqs(field: str, values: Iterable[str | bool | float], boost: float = 1.0) -> Clause:
    return {"terms": {field: list(values), "boost": boost}}


def filter_query(
    scope_dataset: Dataset,
    shoulds: list[Clause],
    schema: Schema | None = None,
    filters: Filters = (),
    include_dataset: Sequence[str] = (),
    exclude_schema: Sequence[str] = (),
    exclude_dataset: Sequence[str] = (),
    changed_since: str | None = None,
    exclude_entity_ids: Sequence[str] = (),
    filter_op: Operator = Operator.AND,
) -> Clause:
    filterqs: list[Clause] = []
    must_not: list[Clause] = []

    datasets: set[str] = set(scope_dataset.dataset_names)
    if len(include_dataset):
        datasets = datasets.intersection(include_dataset)
    if len(exclude_dataset):
        # This is logically a bit more consistent, but doesn't describe the use
        # case of wanting to screen all the entities from datasets X, Y but not Z:
        # must_not.append({"term": {"datasets": exclude_ds}})
        datasets = datasets.difference(exclude_dataset)
    if len(datasets):
        filterqs.append(tqs("datasets", datasets))
    else:
        filterqs.append({"match_none": {}})

    if schema is not None:
        schemata = schema.matchable_schemata
        if not schema.matchable:
            schemata.update(schema.descendants)
        names = [s.name for s in schemata]
        filterqs.append(tqs("schema", names))

    filters_agg = defaultdict(list)
    for field, value in filters:
        filters_agg[field].append(value)

    for field, values in filters_agg.items():
        if filter_op == Operator.OR:
            filterqs.append(tqs(field, values))
            continue
        elif filter_op == Operator.AND:
            for v in values:
                filterqs.append(tq(field, v))

    if changed_since is not None:
        filterqs.append({"range": {"last_change": {"gt": changed_since}}})

    for schema_name in exclude_schema:
        must_not.append(tq("schema", schema_name))

    # Exclude entities by any ID in the cluster
    if exclude_entity_ids:
        must_not.append(tqs("entity_id", exclude_entity_ids))
        must_not.append(tqs("referents", exclude_entity_ids))

    return {
        "bool": {
            "filter": filterqs,
            "must_not": must_not,
            "should": shoulds,
            "minimum_should_match": 1,
        }
    }


def names_query(entity: EntityProxy) -> list[Clause]:
    """Build the name clauses of a /match candidate query.

    One `dis_max` per unique comparable name part (exact, fuzzy and symbol channels),
    one `terms` clause over the space-less forms of all names, and one `term` per weak
    alias. A document scores each query part at most once, however many of its aliases
    carry it, and the sum over parts ranks documents by how many query parts they cover.
    """
    # Primary names come before aliases so that a query truncated by MAX_PARTS keeps
    # the parts of its primary names whole.
    primary = set(entity.get("name", quiet=True))
    names = sorted(
        entity_names(entity, is_query=True),
        key=lambda n: (n.original not in primary, n.form),
    )
    part_symbols: dict[str, set[Symbol]] = {}
    joined: set[str] = set()
    for name in names:
        comparables = [part.comparable for part in name.parts]
        for comparable in comparables:
            part_symbols.setdefault(comparable, set())
        for span in name.spans:
            for part in span.parts:
                part_symbols[part.comparable].add(span.symbol)
        if len(comparables) > 0:
            joined.add("".join(comparables))

    shoulds: list[Clause] = []
    for comparable, symbols in list(part_symbols.items())[:MAX_PARTS]:
        channels: list[Clause] = [tq(NAME_PART_FIELD, comparable, NAME_PART_BOOST)]
        variants = sorted(name_part_variants(comparable))
        if len(variants) > 1:
            channels.append(tqs(NAME_VARIANTS_FIELD, variants, VARIANTS_BOOST))
        symbol_ids = sorted(index_symbols(symbols))[:MAX_SYMBOLS_PER_PART]
        if len(symbol_ids) > 0:
            symbol_terms = [tq(NAME_SYMBOLS_FIELD, sym_id) for sym_id in symbol_ids]
            channels.append(
                {"dis_max": {"queries": symbol_terms, "boost": SYMBOL_BOOST}}
            )
        shoulds.append({"dis_max": {"queries": channels, "tie_breaker": 0.0}})

    if len(joined) > 0:
        shoulds.append(tqs(NAME_JOINED_FIELD, sorted(joined), JOINED_BOOST))

    for weak in entity_weak_names(entity):
        shoulds.append(tq(NAME_PART_FIELD, weak, WEAK_ALIAS_BOOST))

    return shoulds


def entity_query(
    dataset: Dataset,
    entity: EntityProxy,
    filters: Filters = (),
    include_dataset: Sequence[str] = (),
    exclude_schema: Sequence[str] = (),
    exclude_dataset: Sequence[str] = (),
    changed_since: str | None = None,
    exclude_entity_ids: Sequence[str] = (),
    filter_op: Operator = Operator.AND,
) -> Clause:
    shoulds: list[Clause] = names_query(entity)
    for prop, value in entity.itervalues():
        if prop.type == registry.name or not prop.matchable:
            continue
        if prop.type == registry.address:
            query = {"match": {prop.type.group: value}}
            shoulds.append(query)
        elif prop.type.group is not None:
            boost = TYPE_BOOSTS.get(prop.type, 1.0)
            shoulds.append(tq(prop.type.group, value, boost))

    return filter_query(
        dataset,
        shoulds,
        filters=filters,
        filter_op=filter_op,
        schema=entity.schema,
        include_dataset=include_dataset,
        exclude_schema=exclude_schema,
        exclude_dataset=exclude_dataset,
        changed_since=changed_since,
        exclude_entity_ids=exclude_entity_ids,
    )


def text_query(
    dataset: Dataset,
    schema: Schema,
    query: str,
    filters: Filters = (),
    fuzzy: bool = False,
    simple: bool = False,
    include_dataset: Sequence[str] = (),
    exclude_schema: Sequence[str] = (),
    exclude_dataset: Sequence[str] = (),
    changed_since: str | None = None,
    exclude_entity_ids: Sequence[str] = (),
    filter_op: Operator = Operator.AND,
) -> Clause:
    if not len(query.strip()):
        should: Clause = {"match_all": {}}
    elif simple:
        should = {
            "simple_query_string": {
                "query": query,
                "fields": ["names^3", "text"],
                "default_operator": "AND",
                "analyzer": "osa-analyzer",
                "lenient": True,
            }
        }
    else:
        should = {
            "query_string": {
                "query": query,
                "fields": ["names^3", "text"],
                "default_operator": "AND",
                "fuzziness": "AUTO" if fuzzy else 0,
                "analyzer": "osa-analyzer",
                "lenient": True,
            }
        }
        # log.info("Query", should=should)
    return filter_query(
        dataset,
        [should],
        schema=schema,
        filters=filters,
        include_dataset=include_dataset,
        exclude_schema=exclude_schema,
        exclude_dataset=exclude_dataset,
        changed_since=changed_since,
        exclude_entity_ids=exclude_entity_ids,
        filter_op=filter_op,
    )


def prefix_query(
    dataset: Dataset,
    prefix: str,
) -> Clause:
    if not len(prefix.strip()):
        should: Clause = {"match_none": {}}
    else:
        should = {"match_phrase_prefix": {"names": {"query": prefix, "slop": 2}}}
    return filter_query(dataset, [should])


def facet_aggregations(fields: Sequence[str] = ()) -> Clause:
    aggs: Clause = {}
    for field in fields:
        aggs[field] = {"terms": {"field": field, "size": 1000}}
    return aggs


def iter_sorts(sorts: Sequence[str]) -> Generator[tuple[str, str], None, None]:
    for sort in sorts:
        order = "asc"
        if ":" in sort:
            sort, order = sort.rsplit(":", 1)
        if order not in ["asc", "desc"]:
            order = "asc"
        yield sort, order


def parse_sorts(
    sorts: Sequence[str], defaults: Sequence[Sort] = DEFAULT_SORTS
) -> list[Any]:
    """Accept sorts of the form: <field>:<order>, e.g. first_seen:desc."""
    objs: list[Sort] = []
    for sort, order in iter_sorts(sorts):
        objs.append({sort: {"order": order, "missing": "_last"}})
    objs.extend(defaults)
    return objs
