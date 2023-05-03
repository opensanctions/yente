from typing import Any, Dict, Optional, TYPE_CHECKING
from followthemoney import model
from followthemoney.model import Model
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.entity import CompositeEntity

from yente.logs import get_logger

if TYPE_CHECKING:
    from yente.data.common import EntityExample

log = get_logger(__name__)


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, model: Model, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(model, data, cleaned=cleaned)
        self.target: bool = data.get("target", False)
        self._first_seen: Optional[str] = data.get("first_seen", None)
        self._last_seen: Optional[str] = data.get("last_seen", None)

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["target"] = self.target
        if data.get("first_seen") is None:
            data["first_seen"] = self._first_seen
        if data.get("last_seen") is None:
            data["last_seen"] = self._last_seen
        return data

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
