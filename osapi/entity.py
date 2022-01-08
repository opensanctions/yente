from typing import Any, Dict
from followthemoney import model
from nomenklatura.entity import CompositeEntity
from nomenklatura.dataset import Dataset as NomenklaturaDataset

from osapi.util import iso_datetime


class Dataset(NomenklaturaDataset):
    def __init__(self, data: Dict[str, Any]):
        super().__init__(name=data.get("name"), title=data.get("title"))
        self.last_export = iso_datetime(data["last_export"])


Datasets = Dict[str, Dataset]


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects."""

    def __init__(self, data: Dict[str, Any]):
        super().__init__(model, data, cleaned=True)
        self.referents.update(data.get("referents", []))
        self.target: bool = data.get("target", False)
        self.first_seen: str = data.get("first_seen")
        self.last_seen: str = data.get("last_seen")

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["target"] = self.target
        data["caption"] = self.caption
        return data

    @classmethod
    def from_data(cls, data: Dict[str, Any], datasets: Datasets) -> "Entity":
        obj = cls(data)
        for dataset in data.get("datasets", []):
            obj.datasets.add(datasets[dataset])
        return obj
