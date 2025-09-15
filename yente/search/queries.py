import enum
from pprint import pprint  # noqa
from collections import defaultdict
from typing import Any, Dict, Generator, Iterable, List, Set, Tuple, Union, Optional
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from rigour.names import Symbol

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.util import entity_names, index_symbol, is_matchable_symbol, pick_names
from yente.search.mapping import NAME_SYMBOLS_FIELD, NAMES_FIELD
from yente.search.mapping import NAME_PART_FIELD, NAME_PHONETIC_FIELD

log = get_logger(__name__)
Clause = Dict[str, Any]
FilterSpec = Tuple[str, Union[str, bool]]
Filters = List[FilterSpec]
Sort = Union[str, Dict[str, Dict[str, str]]]

DEFAULT_SORTS: List[Sort] = [
    {"_score": {"order": "desc"}},
    {"entity_id": {"order": "asc", "unmapped_type": "keyword"}},
]

# Boost factors for symbol categories to demote low-information name parts.
SYMBOL_BOOSTS = {
    Symbol.Category.NUMERIC: 1.4,
    Symbol.Category.LOCATION: 1.1,
    Symbol.Category.ORG_CLASS: 0.7,
    Symbol.Category.SYMBOL: 0.8,
}


class Operator(str, enum.Enum):
    AND = "AND"
    OR = "OR"


def tq(field: str, value: str | bool, boost: float = 1.0) -> Clause:
    return {"term": {field: {"value": value, "boost": boost}}}


def tqs(field: str, values: Iterable[str | bool | float]) -> Clause:
    return {"terms": {field: list(values)}}


def filter_query(
    scope_dataset: Dataset,
    shoulds: List[Clause],
    schema: Optional[Schema] = None,
    filters: Filters = [],
    include_dataset: List[str] = [],
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
    exclude_entity_ids: List[str] = [],
    filter_op: Operator = Operator.AND,
) -> Clause:
    filterqs: List[Clause] = []
    must_not: List[Clause] = []

    datasets: Set[str] = set(scope_dataset.dataset_names)
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


def names_query(entity: EntityProxy) -> List[Clause]:
    names = entity.get_type_values(registry.name, matchable=True)
    shoulds: List[Clause] = []
    for picked_name in pick_names(names, limit=5):
        match = {
            NAMES_FIELD: {
                "query": picked_name,
                "operator": "AND",
                "boost": 3.0,
            }
        }
        if settings.MATCH_FUZZY:
            match[NAMES_FIELD]["fuzziness"] = "AUTO"
        shoulds.append({"match": match})

    seen: Set[str] = set()
    for name in entity_names(entity):
        part_symbols: Dict[str, Set[Symbol]] = defaultdict(set)
        for span in name.spans:
            for part in span.parts:
                part_symbols[part.form].add(span.symbol)
        for part in name.parts:
            if part.comparable in seen:
                continue
            seen.add(part.comparable)

            # The idea here is to rank down the contribution to the score of less interesting name parts
            # To some degree, this is already done by the IDF component of the ES scoring algorithm
            # (which reduces the influence of frequent terms), but that doesn't work too well for e.g.
            # less common languages.
            symbols: Set[Symbol] = part_symbols.get(part.form, set())
            boosts = [
                SYMBOL_BOOSTS[symbol.category]
                for symbol in symbols
                if symbol.category in SYMBOL_BOOSTS
            ]
            boost = max(boosts, default=1.0)

            # We have multiple ways to query for a name part (verbatim, comparable, phonetic, symbol)
            # In the end, we dis-max them to get the one that works best, but not give an outsized important
            # to this name part just because multiple variants match.
            query_variants: List[Clause] = []
            query_variants.append(tq(NAME_PART_FIELD, part.form, boost))
            if part.comparable != part.form:
                query_variants.append(tq(NAME_PART_FIELD, part.comparable, boost * 0.9))

            metaphone = part.metaphone
            if metaphone is not None and len(metaphone) > 2:
                query_variants.append(tq(NAME_PHONETIC_FIELD, metaphone, boost * 0.5))

            for symbol in symbols:
                if is_matchable_symbol(symbol):
                    query_variants.append(
                        tq(NAME_SYMBOLS_FIELD, index_symbol(symbol), boost * 0.7)
                    )

            query = {"dis_max": {"queries": query_variants, "tie_breaker": 0.2}}
            shoulds.append(query)

        # TODO: query by key?
    return shoulds


def entity_query(
    dataset: Dataset,
    entity: EntityProxy,
    filters: Filters = [],
    include_dataset: List[str] = [],
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
    exclude_entity_ids: List[str] = [],
    filter_op: Operator = Operator.AND,
) -> Clause:
    shoulds: List[Clause] = names_query(entity)
    for prop, value in entity.itervalues():
        if prop.type == registry.name or not prop.matchable:
            continue
        if prop.type == registry.address:
            query = {"match": {prop.type.group: value}}
            shoulds.append(query)
        elif prop.type.group is not None:
            shoulds.append(tq(prop.type.group, value))

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
    filters: Filters = [],
    fuzzy: bool = False,
    simple: bool = False,
    include_dataset: List[str] = [],
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
    exclude_entity_ids: List[str] = [],
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


def facet_aggregations(fields: List[str] = []) -> Clause:
    aggs: Clause = {}
    for field in fields:
        aggs[field] = {"terms": {"field": field, "size": 1000}}
    return aggs


def iter_sorts(sorts: List[str]) -> Generator[Tuple[str, str], None, None]:
    for sort in sorts:
        order = "asc"
        if ":" in sort:
            sort, order = sort.rsplit(":", 1)
        if order not in ["asc", "desc"]:
            order = "asc"
        yield sort, order


def parse_sorts(sorts: List[str], defaults: List[Sort] = DEFAULT_SORTS) -> List[Any]:
    """Accept sorts of the form: <field>:<order>, e.g. first_seen:desc."""
    objs: List[Sort] = []
    for sort, order in iter_sorts(sorts):
        objs.append({sort: {"order": order, "missing": "_last"}})
    objs.extend(defaults)
    return objs
