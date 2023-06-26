from typing import Any, Dict, Set, Optional, TYPE_CHECKING
from followthemoney import model
from followthemoney.model import Model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.publish.names import pick_name

from yente.logs import get_logger

if TYPE_CHECKING:
    from yente.data.common import EntityExample

log = get_logger(__name__)


class Entity(EntityProxy):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, model: Model, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(model, data, cleaned=cleaned)
        self._caption: str = data.get("caption", None)
        if self._caption is None:
            self._caption = self._pick_caption()
        self.target: bool = data.get("target", False)
        self.first_seen: Optional[str] = data.get("first_seen", None)
        self.last_seen: Optional[str] = data.get("last_seen", None)
        self.last_change: Optional[str] = data.get("last_change", None)
        self.datasets: Set[str] = set(data.get("datasets", []))
        self.referents: Set[str] = set(data.get("referents", []))
        self.context = {}

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

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["caption"] = self._caption
        data["target"] = self.target
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["last_change"] = self.last_change
        data["datasets"] = list(self.datasets)
        data["referents"] = list(self.referents)
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
