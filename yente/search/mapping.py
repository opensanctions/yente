from typing import Iterable
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry

DATE_FORMAT = "yyyy-MM-dd'T'HH||yyyy-MM-dd'T'HH:mm||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy"
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
    "index": {"refresh_interval": "5s"},
}


def make_field(type_, copy_to=None, format=None):
    spec = {"type": type_}
    if type_ == "keyword":
        spec["normalizer"] = "osa-normalizer"
    if type_ == "text":
        spec["analyzer"] = "osa-analyzer"
    if copy_to is not None and copy_to is not False:
        spec["copy_to"] = copy_to
    if format is not None:
        spec["format"] = format
    return spec


def make_type_field(type_, copy_to=True):
    if type_ == registry.date:
        return make_field("date", copy_to=copy_to, format=DATE_FORMAT)
    strong = type_.group is not None
    field_type = "keyword" if strong else "text"
    if type_ in TEXT_TYPES:
        field_type = "text"
    return make_field(field_type, copy_to=copy_to)


def make_entity_mapping(schemata: Iterable[Schema]):
    prop_mapping = {}
    for schema_name in schemata:
        schema = model.get(schema_name)
        assert schema is not None, schema_name
        for name, prop in schema.properties.items():
            if prop.stub:
                continue
            copy_to = ["text"]
            if prop.type.group is not None:
                copy_to.append(prop.type.group)
            prop_mapping[name] = make_type_field(prop.type, copy_to=copy_to)

    mapping = {
        "canonical_id": make_field("keyword"),
        "schema": make_field("keyword"),
        "caption": make_field("keyword", copy_to=["names", "text"]),
        "datasets": make_field("keyword"),
        "referents": make_field("keyword"),
        "target": make_field("boolean"),
        "text": make_field("text"),
        "last_seen": make_field("date", format=DATE_FORMAT),
        "first_seen": make_field("date", format=DATE_FORMAT),
        "properties": {"dynamic": "strict", "properties": prop_mapping},
    }
    for t in registry.groups.values():
        if t.group is None:
            continue
        mapping[t.group] = make_type_field(t, copy_to="text")

    drop_fields = [t.group for t in registry.groups.values()]
    drop_fields.append("text")
    return {
        "dynamic": "strict",
        "properties": mapping,
        "_source": {"excludes": drop_fields},
    }


def make_statement_mapping():
    mapping = {
        "canonical_id": {"type": "keyword"},
        "entity_id": {"type": "keyword"},
        "prop": {"type": "keyword"},
        "prop_type": {"type": "keyword"},
        "schema": {"type": "keyword"},
        "value": {
            "type": "keyword",
            "fields": {"text": {"type": "text", "analyzer": "osa-analyzer"}},
        },
        "dataset": {"type": "keyword"},
        "target": {"type": "boolean"},
        "unique": {"type": "boolean"},
        "last_seen": make_field("date", format=DATE_FORMAT),
        "first_seen": make_field("date", format=DATE_FORMAT),
    }
    return {
        "dynamic": "strict",
        "properties": mapping,
    }
