from pathlib import Path
from normality import slugify
from pydantic import BaseModel, FileUrl, parse_obj_as, validator
from functools import cached_property
from typing import AsyncGenerator, Dict, List, Optional, Set
from nomenklatura.dataset import Dataset as NomenklaturaDataset

from yente.data.entity import Entity
from yente.data.loader import URL, load_json_lines


class DatasetManifest(BaseModel):
    name: str
    title: str
    path: Optional[Path]
    url: Optional[URL]
    version: Optional[str]
    namespace: bool = False
    children: List[str] = []

    @validator("name")
    def name_is_slug(cls, v):
        norm = slugify(v, sep="_")
        if v != norm:
            raise ValueError("invalid dataset name (try: %s)" % norm)
        return v

    @validator("url", always=True)
    def url_from_path(cls, v, values):
        if v is None and values["path"] is not None:
            file_url = values["path"].resolve().as_uri()
            v = parse_obj_as(FileUrl, file_url)
        return v


class Dataset(NomenklaturaDataset):
    def __init__(self, index: "Datasets", manifest: DatasetManifest):
        super().__init__(name=manifest.name, title=manifest.title)
        self.index = index
        self.manifest = manifest
        self.version = manifest.version
        self.is_loadable = self.manifest.url is not None

    @cached_property
    def children(self) -> Set["Dataset"]:
        children: Set["Dataset"] = set()
        for child_name in self.manifest.children:
            children.add(self.index[child_name])
        return children

    @cached_property
    def datasets(self) -> Set["Dataset"]:
        datasets: Set["Dataset"] = set([self])
        for child in self.children:
            datasets.update(child.datasets)
        return datasets

    @property
    def dataset_names(self) -> List[str]:
        return [d.name for d in self.datasets]

    async def entities_from_url(self) -> AsyncGenerator[Entity, None]:
        if self.manifest.url is None:
            return
        async for data in load_json_lines(self.manifest.url):
            entity = Entity.from_os_data(data, self.index)
            # TODO: set last_seen, first_seen
            if not len(entity.datasets):
                entity.datasets.add(self)
            yield entity

    async def entities(self) -> AsyncGenerator[Entity, None]:
        # TODO: support for mappings
        # TODO: support for namespaces
        if self.manifest.url is not None:
            async for entity in self.entities_from_url():
                yield entity


Datasets = Dict[str, Dataset]
