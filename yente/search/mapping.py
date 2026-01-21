from collections import defaultdict
import itertools
from typing import Any, Dict, Iterable, List, Optional, Union
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.types.common import PropertyType
from followthemoney.types.name import NameType

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)

MappingProperty = Dict[str, Union[List[str], str]]

DATE_FORMAT = "yyyy-MM-dd'T'HH||yyyy-MM-dd'T'HH:mm||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy||strict_date_optional_time"  # noqa
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
        # Amazon OpenSearch Serverless has a minimum of 10s
        "refresh_interval": "10s",
        "auto_expand_replicas": settings.INDEX_AUTO_REPLICAS,
        "number_of_shards": settings.INDEX_SHARDS,
        "similarity": {
            # We use this for names, to avoid over-penalizing entities with many names.
            # For example, for the query "Hamas", we don't want to penalize our canonical Hamas entity
            # for having many other names (like "Izz al-Din al-Qassam-brigade"). Otherwise, a
            # non-deduped Hamas entity that just has a single names always ranks higher up.
            "weak_length_norm": {
                # BM25 is the default similarity algorithm.
                "type": "BM25",
                # 0.75 is the default. This is a bit of a jiggle factor together with the up-scoring
                # we do for large documents. With the values chosen for that in Aug 2025, we get
                # the following behavior:
                #  - Hamas needs 0.25 to rank first for "Hamas".
                #  - Putin starts ranking first from around 0.5 (probably because the doc is just so
                #    huge, around 2x of Obama)
                #  - Obama needs 0.2 to rank first. Lots of non-latin names but overall, the document
                #    isn't that large.
                "b": 0.25,
            }
        },
    },
}
NAMES_FIELD = NameType.group or "names"
NAME_PART_FIELD = "name_parts"
NAME_SYMBOLS_FIELD = "name_symbols"
NAME_PHONETIC_FIELD = "name_phonetic"
# NAME_KEY_FIELD = "name_keys"


def make_field(
    type_: str,
    copy_to: Optional[List[str]] = None,
    format: Optional[str] = None,
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
    type_: PropertyType,
    copy_to: Optional[List[str]] = None,
) -> MappingProperty:
    field_type = "keyword" if type_.group else "text"
    if type_ in TEXT_TYPES:
        field_type = "text"
    return make_field(field_type, copy_to=copy_to)


def make_keyword() -> MappingProperty:
    return {"type": "keyword"}


def make_entity_mapping(schemata: Optional[Iterable[Schema]] = None) -> Dict[str, Any]:
    if schemata is None:
        schemata = list(model.schemata.values())
    # Collect field definitions:
    # Multiple schemata can have the same property name, but we flatten them
    # into a single field in the search index. That's why we collect a list of
    # fields for each property name first and resolve them later.
    prop_name_to_fields: Dict[str, List[MappingProperty]] = defaultdict(list)
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
            prop_name_to_fields[name].append(
                make_type_field(prop.type, copy_to=copy_to)
            )

    # Resolve list of field definitions to a single definition per field name
    prop_mapping: Dict[str, MappingProperty] = {}
    for prop_name, fields in prop_name_to_fields.items():
        merged_copy_to = list(
            set(itertools.chain.from_iterable([f["copy_to"] for f in fields]))
        )
        text_fields = [f for f in fields if f["type"] == "text"]
        keyword_fields = [f for f in fields if f["type"] == "keyword"]

        # All properties with the same name ought to map to the same field definition.
        # If a conflict occurs, we choose field type "keyword" over "text".
        # Currently, only the property "authority" has conflicts (some are string, some are entity)
        selected_field = keyword_fields[-1] if keyword_fields else text_fields[-1]
        # We merge the copy_to fields rather than choosing one value, just so that
        # queries work as expected in case one property maps to different copy_to than another
        # with the same field name.
        selected_field["copy_to"] = merged_copy_to

        prop_mapping[prop_name] = selected_field

    mapping: Dict[str, Any] = {
        "schema": make_keyword(),
        "caption": make_field("keyword"),
        "entity_id": make_field("keyword"),
        "datasets": make_keyword(),
        "referents": make_keyword(),
        "target": make_field("boolean"),
        "origin": make_keyword(),
        "text": make_field("text"),
        "entity_values_count": make_field("integer"),
        NAME_PHONETIC_FIELD: make_keyword(),
        NAME_PART_FIELD: make_field("keyword", copy_to=["text"]),
        # NAME_KEY_FIELD: make_field("keyword"),
        NAME_SYMBOLS_FIELD: make_field("keyword"),
        "last_change": make_field("date", format=DATE_FORMAT),
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

    # Weaker length normalization for names. Merged entities have a lot of names,
    # and we don't want to penalize them for that.
    mapping[NAMES_FIELD]["similarity"] = "weak_length_norm"

    # These fields will be pruned from the _source field after the document has been
    # indexed, but before the _source field is stored. We can still search on these fields,
    # even though they are not in the stored and returned _source.
    drop_fields = [t.group for t in registry.groups.values()]
    drop_fields.append("text")
    drop_fields.append(NAME_PHONETIC_FIELD)
    drop_fields.append(NAME_PART_FIELD)
    # drop_fields.append(NAME_KEY_FIELD)
    drop_fields.append(NAME_SYMBOLS_FIELD)
    drop_fields.remove(NAMES_FIELD)
    return {
        "dynamic": "strict",
        "properties": mapping,
        "_source": {"excludes": drop_fields},
    }
