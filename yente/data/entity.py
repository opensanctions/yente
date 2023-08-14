from typing import Any, Dict, TYPE_CHECKING
from followthemoney import model
from followthemoney.model import Model
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.publish.names import pick_caption
from nomenklatura.stream import StreamEntity

from yente.logs import get_logger

if TYPE_CHECKING:
    from yente.data.common import EntityExample

log = get_logger(__name__)


class Entity(StreamEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, model: Model, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(model, data, cleaned=cleaned)
        if self._caption is None:
            self._caption = pick_caption(self)

    @classmethod
    def from_example(cls, example: "EntityExample") -> "Entity":
        data = {"id": example.id, "schema": example.schema_}
        obj = cls(model, data)
        for prop_name, values in example.properties.items():
            if prop_name not in obj.schema.properties:
                log.warning(
                    "Invalid example property",
                    prop=prop_name,
                    value=str(values),
                )
                continue
            obj.add(prop_name, values, cleaned=False, fuzzy=True)

        # Generate names from name parts
        combine_names(obj)

        # Extract names from IBANs, phone numbers etc.
        countries = obj.get_type_values(registry.country)
        for (prop, value) in list(obj.itervalues()):
            hint = prop.type.country_hint(value)
            if hint is not None and hint not in countries:
                obj.add("country", hint, cleaned=True)
        return obj
