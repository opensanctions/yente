from typing import Any, Dict, Generator, List, Tuple, Union, Optional
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney.types.name import NameType

from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.util import tokenize_names, pick_names
from yente.search.mapping import TEXT_TYPES

log = get_logger(__name__)
FilterDict = Dict[str, Union[bool, str, List[str]]]
Clause = Dict[str, Any]

NAMES_FIELD = NameType.group or "names"


def filter_query(
    shoulds: List[Clause],
    dataset: Optional[Dataset] = None,
    schema: Optional[Schema] = None,
    filters: FilterDict = {},
) -> Clause:
    filterqs: List[Clause] = []
    if dataset is not None:
        filterqs.append({"terms": {"datasets": dataset.dataset_names}})
    if schema is not None:
        schemata = schema.matchable_schemata
        schemata.add(schema)
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
    return {"bool": {"filter": filterqs, "should": shoulds, "minimum_should_match": 1}}


def names_query(entity: EntityProxy, field: str = NAMES_FIELD) -> List[Clause]:
    names = entity.get_type_values(registry.name, matchable=True)
    shoulds = []
    for name in pick_names(names, limit=5):
        match = {
            field: {
                "query": name,
                "fuzziness": "AUTO",
                "minimum_should_match": "70%",
                "boost": 3.0,
            }
        }
        shoulds.append({"match": match})
    for token in tokenize_names(names):
        shoulds.append({"term": {field: {"value": token}}})
    return shoulds


def entity_query(dataset: Dataset, entity: EntityProxy) -> Clause:
    shoulds: List[Clause] = []
    for prop, value in entity.itervalues():
        if prop.type == registry.name or not prop.matchable:
            continue
        if prop.type in TEXT_TYPES:
            query = {"match": {prop.type.group: {"query": value}}}
            shoulds.append(query)
        elif prop.type.group is not None:
            shoulds.append({"term": {prop.type.group: value}})
        else:
            shoulds.append({"match_phrase": {"text": value}})

    shoulds.extend(names_query(entity))
    return filter_query(shoulds, dataset=dataset, schema=entity.schema)


def text_query(
    dataset: Dataset,
    schema: Schema,
    query: str,
    filters: FilterDict = {},
    fuzzy: bool = False,
    simple: bool = False,
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
    return filter_query([should], dataset=dataset, schema=schema, filters=filters)


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
