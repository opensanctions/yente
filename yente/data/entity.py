from typing import TYPE_CHECKING, Any, Dict
from followthemoney import model
from followthemoney.types import registry
from followthemoney.helpers import combine_names
from nomenklatura.entity import CompositeEntity

if TYPE_CHECKING:
    from yente.data.dataset import Datasets


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(model, data, cleaned=cleaned)
        self._caption: str = data.get("caption")
        self.referents.update(data.get("referents", []))
        self.target: bool = data.get("target", False)
        self.first_seen: str = data.get("first_seen")
        self.last_seen: str = data.get("last_seen")

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["target"] = self.target
        data["caption"] = self._caption
        return data

    @classmethod
    def from_os_data(
        cls, data: Dict[str, Any], datasets: "Datasets", cleaned: bool = True
    ) -> "Entity":
        obj = cls(data, cleaned=cleaned)
        for name in data.get("datasets", []):
            dataset = datasets.get(name)
            if dataset is not None:
                obj.datasets.add(dataset)
        return obj

    @classmethod
    def from_example(cls, schema: str, properties: Dict[str, Any]) -> "Entity":
        data = {"id": "example", "schema": schema}
        obj = cls(data)
        for prop_name, values in properties.items():
            obj.add(prop_name, values, cleaned=False, fuzzy=True)

        # Generate names from name parts
        combine_names(obj)

        # Extract names from IBANs, phone numbers etc.
        countries = obj.get_type_values(registry.country)
        for (prop, value) in obj.itervalues():
            hint = prop.type.country_hint(value)
            if hint is not None and hint not in countries:
                obj.add("country", hint, cleaned=True)
        return obj
