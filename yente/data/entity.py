from typing import Any, Dict, Optional
from followthemoney import model
from followthemoney.model import Model
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.entity import CompositeEntity

from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, model: Model, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(model, data, cleaned=cleaned)
        self._caption: str = data.get("caption") or self.caption
        self.target: bool = data.get("target", False)

        # Not sure that impugning this is a good idea, to be seen:
        first_seen: Optional[str] = data.get("first_seen")
        last_seen: Optional[str] = data.get("last_seen")
        self.first_seen: str = first_seen or last_seen or settings.RUN_TIME
        self.last_seen: str = last_seen or self.first_seen

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["target"] = self.target
        data["caption"] = self._caption
        return data

    @classmethod
    def from_example(cls, schema: str, properties: Dict[str, Any]) -> "Entity":
        data = {"id": "example", "schema": schema}
        obj = cls(model, data)
        for prop_name, values in properties.items():
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
