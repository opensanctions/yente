import logging
from normality import collapse_spaces
from typing import Any, Dict, List, Union, Optional
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

from yente.data.dataset import Dataset
from yente.search.mapping import TEXT_TYPES

log = logging.getLogger(__name__)

FilterDict = Dict[str, Union[bool, str, List[str]]]


def filter_query(
    shoulds,
    dataset: Optional[Dataset] = None,
    schema: Optional[Schema] = None,
    filters: FilterDict = {},
):
    filterqs = []
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


def names_query(entity: EntityProxy) -> List[Dict[str, Any]]:
    names = entity.get_type_values(registry.name, matchable=True)
    # When there are a limited number of names, try fuzzy matching in the
    # index:
    if len(names) < 5:
        shoulds = []
        for name in names:
            query = {
                "match": {
                    registry.name.group: {
                        "query": name,
                        "fuzziness": "AUTO",
                        "minimum_should_match": 1,
                        "boost": 3.0,
                    }
                }
            }
            shoulds.append(query)
        return shoulds
    # Deduplicate names before making them a match query. This is a hack to
    # work around a low default for `query.bool.max_clause_count` in Elastic
    # cloud. I'm not sure if overall it's an improvement to the query perf or
    # recall on entity matching.
    normalized_ = [collapse_spaces(n.lower()) for n in names]
    normalized = set([n for n in normalized_ if n is not None])
    # This query is non-fuzzy:
    query = {
        "match": {
            registry.name.group: {
                "query": " ".join(normalized),
                "minimum_should_match": 1,
                "boost": 3.0,
            }
        }
    }
    return [query]


def entity_query(dataset: Dataset, entity: EntityProxy):
    shoulds: List[Dict[str, Any]] = []
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
):

    if not len(query.strip()):
        should = {"match_all": {}}
    else:
        should = {
            "query_string": {
                "query": query,
                "fields": ["names^3", "text"],
                "default_operator": "and",
                "fuzziness": "AUTO" if fuzzy else 0,
                "lenient": fuzzy,
            }
        }
    return filter_query([should], dataset=dataset, schema=schema, filters=filters)


def prefix_query(
    dataset: Dataset,
    prefix: str,
):
    if not len(prefix.strip()):
        should = {"match_none": {}}
    else:
        should = {"match_phrase_prefix": {"names": {"query": prefix, "slop": 2}}}
    return filter_query([should], dataset=dataset)


def facet_aggregations(fields: List[str] = []) -> Dict[str, Any]:
    aggs = {}
    for field in fields:
        aggs[field] = {"terms": {"field": field, "size": 1000}}
    return aggs


def parse_sorts(sorts: List[str], default: Optional[str] = "_score") -> List[Any]:
    """Accept sorts of the form: <field>:<order>, e.g. first_seen:desc."""
    objs: List[Any] = []
    for sort in sorts:
        order = "asc"
        if ":" in sort:
            sort, order = sort.rsplit(":", 1)
        obj = {sort: {"order": order, "missing": "_last"}}
        objs.append(obj)
    if default is not None:
        objs.append(default)
    return objs
