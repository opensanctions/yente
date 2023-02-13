from typing import Any, Dict, Iterable, List, Optional, Union
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.types.common import PropertyType

from yente import settings

MappingProperty = Dict[str, Union[List[str], str]]

DATE_FORMAT = "yyyy-MM-dd'T'HH||yyyy-MM-dd'T'HH:mm||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy||strict_date_optional_time"
TEXT_TYPES = (registry.name, registry.address)
INDEX_SETTINGS = {
    "analysis": {
        "normalizer": {
            "osa-normalizer": {
                "type": "custom",
                "filter": ["lowercase", "asciifolding"],
            }
        },
        "analyzer": {
            "osa-analyzer": {
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"],
            }
        },
    },
    "index": {
        "refresh_interval": "5s",
        "auto_expand_replicas": "0-all",
        "number_of_shards": settings.ES_SHARDS,
    },
}


def make_field(
    type_: str, copy_to: Optional[List[str]] = None, format: Optional[str] = None
) -> MappingProperty:
    spec: MappingProperty = {"type": type_}
    if type_ == "keyword":
        spec["normalizer"] = "osa-normalizer"
    if type_ == "text":
        spec["analyzer"] = "osa-analyzer"
    if copy_to is not None:
        spec["copy_to"] = copy_to
    if format is not None:
        spec["format"] = format
    return spec


def make_type_field(
    type_: PropertyType, copy_to: Optional[List[str]] = None
) -> MappingProperty:
    field_type = "keyword" if type_.group else "text"
    if type_ in TEXT_TYPES:
        field_type = "text"
    return make_field(field_type, copy_to=copy_to)


def make_keyword() -> MappingProperty:
    return {"type": "keyword"}


def make_entity_mapping(schemata: Iterable[Schema]) -> Dict[str, Any]:
    prop_mapping: Dict[str, MappingProperty] = {}
    for schema_name in schemata:
        schema = model.get(schema_name)
        assert schema is not None, schema_name
        for name, prop in schema.properties.items():
            if prop.stub:
                continue
            copy_to = ["text"]
            # Do not copy properties which have been specifically
            # excluded from matchable types:
            excluded = prop.type.matchable and not prop.matchable
            # Some types (like topics) are not matchable, but we still want
            # to facet on them.
            if prop.type.group is not None and not excluded:
                copy_to.append(prop.type.group)
            prop_mapping[name] = make_type_field(prop.type, copy_to=copy_to)

    mapping = {
        "schema": make_keyword(),
        "caption": make_field("keyword"),
        "datasets": make_keyword(),
        "referents": make_keyword(),
        "target": make_field("boolean"),
        "text": make_field("text"),
        "last_seen": make_field("date", format=DATE_FORMAT),
        "first_seen": make_field("date", format=DATE_FORMAT),
        "properties": {"dynamic": "strict", "properties": prop_mapping},
    }

    for t in registry.groups.values():
        if t.group is None:
            continue
        if t.group in mapping:
            raise RuntimeError("Double mapping field: %s" % t.group)
        mapping[t.group] = make_type_field(t)

    drop_fields = [t.group for t in registry.groups.values()]
    drop_fields.append("text")
    drop_fields.remove(registry.name.group)
    return {
        "dynamic": "strict",
        "properties": mapping,
        "_source": {"excludes": drop_fields},
    }
