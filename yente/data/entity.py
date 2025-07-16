from typing import Any, Dict, TYPE_CHECKING
from followthemoney import Schema, registry, model, ValueEntity
from followthemoney.exc import InvalidData
from followthemoney.helpers import combine_names
from rigour.names import pick_name

from yente import settings
from yente.logs import get_logger

if TYPE_CHECKING:
    from yente.data.common import EntityExample

log = get_logger(__name__)


class Entity(ValueEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, schema: Schema, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(schema, data, cleaned=cleaned)
        if self._caption is None:
            self._caption = self._pick_caption()

    def _pick_caption(self) -> str:
        is_thing = self.schema.is_a("Thing")
        for prop in self.schema.caption:
            values = self.get(prop)
            if is_thing and len(values) > 1:
                name = pick_name(values)
                if name is not None:
                    return name
            for value in values:
                return value
        return self.schema.label

    @classmethod
    def from_example(cls, example: "EntityExample") -> "Entity":
        data = {"id": example.id, "schema": example.schema_}
        schema = model.get(example.schema_)
        if schema is None:
            raise InvalidData(f"Unknown schema: {example.schema_!r}")
        obj = cls(schema, data)
        if obj.schema.name != settings.BASE_SCHEMA and not obj.schema.matchable:
            raise TypeError("Non-matchable schema for query: %s" % obj.schema.name)

        for prop_name, values in example.properties.items():
            if prop_name not in obj.schema.properties:
                log.warning(
                    "Invalid query property",
                    prop=prop_name,
                    value=repr(values),
                )
                continue
            obj.add(prop_name, values, cleaned=False, fuzzy=True)

        # Generate names from name parts
        combine_names(obj)

        # Extract names from IBANs, phone numbers etc.
        countries = obj.get_type_values(registry.country)
        for prop, value in list(obj.itervalues()):
            hint = prop.type.country_hint(value)
            if hint is not None and hint not in countries:
                obj.add("country", hint, cleaned=True)
        return obj
