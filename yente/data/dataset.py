from typing import Any, Dict, Optional
from nomenklatura.dataset import Dataset as NomenklaturaDataset

from yente.util import iso_datetime


class Dataset(NomenklaturaDataset):
    def __init__(self, data: Dict[str, Any]):
        name = data.get("name")
        super().__init__(name=name, title=data.get("title"))
        self.last_export = iso_datetime(data["last_export"])

        self.entities_url: Optional[str] = data.get("entities_url")
        for resource in data.get("resources", []):
            if resource.get("path") == "entities.ftm.json":
                self.entities_url = resource.get("url")

        self.source_names = data.get("sources", [name])


Datasets = Dict[str, Dataset]
