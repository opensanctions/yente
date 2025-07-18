import enum
from pprint import pprint  # noqa
from collections import defaultdict
from typing import Any, Dict, Generator, List, Set, Tuple, Union, Optional
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.util import pick_names, phonetic_names
from yente.data.util import index_name_parts, index_name_keys
from yente.search.mapping import NAMES_FIELD, NAME_PHONETIC_FIELD
from yente.search.mapping import NAME_PART_FIELD, NAME_KEY_FIELD
from yente import settings

log = get_logger(__name__)
Clause = Dict[str, Any]
FilterSpec = Tuple[str, Union[str, bool]]
Filters = List[FilterSpec]
Sort = Union[str, Dict[str, Dict[str, str]]]

DEFAULT_SORTS: List[Sort] = [
    {"_score": {"order": "desc"}},
    {"entity_id": {"order": "asc", "unmapped_type": "keyword"}},
]


class Operator(str, enum.Enum):
    AND = "AND"
    OR = "OR"


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
        filterqs.append({"terms": {"datasets": list(datasets)}})
    else:
        filterqs.append({"match_none": {}})

    if schema is not None:
        schemata = schema.matchable_schemata
        if not schema.matchable:
            schemata.update(schema.descendants)
        names = [s.name for s in schemata]
        filterqs.append({"terms": {"schema": names}})

    filters_agg = defaultdict(list)
    for field, value in filters:
        filters_agg[field].append(value)

    for field, values in filters_agg.items():
        if filter_op == Operator.OR:
            filterqs.append({"terms": {field: values}})
            continue
        elif filter_op == Operator.AND:
            for v in values:
                filterqs.append({"term": {field: v}})

    if changed_since is not None:
        filterqs.append({"range": {"last_change": {"gt": changed_since}}})

    for schema_name in exclude_schema:
        must_not.append({"term": {"schema": schema_name}})

    # Exclude entities by any ID in the cluster
    if exclude_entity_ids:
        must_not.append({"terms": {"entity_id": exclude_entity_ids}})
        must_not.append({"terms": {"referents": exclude_entity_ids}})

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
    names.extend(entity.get("weakAlias", quiet=True))
    shoulds: List[Clause] = []
    for name in pick_names(names, limit=5):
        match = {
            NAMES_FIELD: {
                "query": name,
                "operator": "AND",
                "boost": 3.0,
            }
        }
        if settings.MATCH_FUZZY:
            match[NAMES_FIELD]["fuzziness"] = "AUTO"
        shoulds.append({"match": match})
    for key in index_name_keys(entity.schema, names):
        term = {NAME_KEY_FIELD: {"value": key, "boost": 4.0}}
        shoulds.append({"term": term})
    for token in index_name_parts(entity.schema, names):
        term = {NAME_PART_FIELD: {"value": token, "boost": 1.0}}
        shoulds.append({"term": term})
    for phoneme in phonetic_names(entity.schema, names):
        term = {NAME_PHONETIC_FIELD: {"value": phoneme, "boost": 0.8}}
        shoulds.append({"term": term})
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
            shoulds.append({"term": {prop.type.group: value}})

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
