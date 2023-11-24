from typing import Any, Dict, Generator, List, Tuple, Union, Optional
from fingerprints import clean_name_light
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.util import pick_names, phonetic_names, index_name_parts
from yente.search.mapping import NAMES_FIELD, NAME_PHONETIC_FIELD
from yente.search.mapping import NAME_PART_FIELD, NAME_KEY_FIELD

log = get_logger(__name__)
FilterDict = Dict[str, Union[bool, str, List[str]]]
Clause = Dict[str, Any]


def filter_query(
    shoulds: List[Clause],
    dataset: Optional[Dataset] = None,
    schema: Optional[Schema] = None,
    filters: FilterDict = {},
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
) -> Clause:
    filterqs: List[Clause] = []
    if dataset is not None:
        ds = [d for d in dataset.dataset_names if d not in exclude_dataset]
        filterqs.append({"terms": {"datasets": ds}})
    if schema is not None:
        schemata = schema.matchable_schemata
        if not schema.matchable:
            schemata.update(schema.descendants)
        names = [s.name for s in schemata]
        filterqs.append({"terms": {"schema": names}})
    for field, values in filters.items():
        if isinstance(values, (bool, str)):
            filterqs.append({"term": {field: {"value": values}}})
            continue
        values = [v for v in values if len(v)]
        if len(values):
            filterqs.append({"terms": {field: values}})
    if changed_since is not None:
        filterqs.append({"range": {"last_change": {"gt": changed_since}}})
    must_not: List[Clause] = []
    for schema_name in exclude_schema:
        must_not.append({"term": {"schema": schema_name}})
    return {
        "bool": {
            "filter": filterqs,
            "must_not": must_not,
            "should": shoulds,
            "minimum_should_match": 1,
        }
    }


def names_query(entity: EntityProxy, fuzzy: bool = True) -> List[Clause]:
    names = entity.get_type_values(registry.name, matchable=True)
    names.extend(entity.get("weakAlias", quiet=True))
    shoulds: List[Clause] = []
    for name in pick_names(names, limit=5):
        match = {
            NAMES_FIELD: {
                "query": name,
                "operator": "AND",
            }
        }
        if fuzzy:
            match[NAMES_FIELD]["fuzziness"] = "AUTO"
        shoulds.append({"match": match})
        cleaned = clean_name_light(name)
        if cleaned is not None:
            term = {NAME_KEY_FIELD: {"value": cleaned, "boost": 4.0}}
            shoulds.append({"term": term})
    for token in set(index_name_parts(names)):
        shoulds.append({"term": {NAME_PART_FIELD: {"value": token}}})
    for phoneme in set(phonetic_names(names)):
        term = {NAME_PHONETIC_FIELD: {"value": phoneme, "boost": 0.8}}
        shoulds.append({"term": term})
    return shoulds


def entity_query(
    dataset: Dataset,
    entity: EntityProxy,
    filters: FilterDict = {},
    fuzzy: bool = True,
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
) -> Clause:
    shoulds: List[Clause] = names_query(entity, fuzzy=fuzzy)
    for prop, value in entity.itervalues():
        if prop.type == registry.name or not prop.matchable:
            continue
        if prop.type == registry.address:
            query = {"match": {prop.type.group: value}}
            shoulds.append(query)
        elif prop.type.group is not None:
            shoulds.append({"term": {prop.type.group: value}})
        elif fuzzy:
            shoulds.append({"match": {"text": value}})

    return filter_query(
        shoulds,
        filters=filters,
        dataset=dataset,
        schema=entity.schema,
        exclude_schema=exclude_schema,
        exclude_dataset=exclude_dataset,
        changed_since=changed_since,
    )


def text_query(
    dataset: Dataset,
    schema: Schema,
    query: str,
    filters: FilterDict = {},
    fuzzy: bool = False,
    simple: bool = False,
    exclude_schema: List[str] = [],
    exclude_dataset: List[str] = [],
    changed_since: Optional[str] = None,
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
        [should],
        dataset=dataset,
        schema=schema,
        filters=filters,
        exclude_schema=exclude_schema,
        exclude_dataset=exclude_dataset,
        changed_since=changed_since,
    )


def prefix_query(
    dataset: Dataset,
    prefix: str,
) -> Clause:
    if not len(prefix.strip()):
        should: Clause = {"match_none": {}}
    else:
        should = {"match_phrase_prefix": {"names": {"query": prefix, "slop": 2}}}
    return filter_query([should], dataset=dataset)


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


def parse_sorts(sorts: List[str], default: Optional[str] = "_score") -> List[Any]:
    """Accept sorts of the form: <field>:<order>, e.g. first_seen:desc."""
    objs: List[Any] = []
    for sort, order in iter_sorts(sorts):
        objs.append({sort: {"order": order, "missing": "_last"}})
    if default is not None:
        objs.append(default)
    return objs
