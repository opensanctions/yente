from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from nomenklatura.dataset import DataCatalog

from yente import settings
from yente.data.loader import load_yaml_url
from yente.data.dataset import Dataset


class CatalogManifest(BaseModel):
    """OpenSanctions is not one dataset but a whole collection, so this
    side-loads it into the yente dataset archive."""

    url: str
    scope: Optional[str] = None
    scopes: List[str] = []
    namespace: Optional[bool] = None
    resource_name: Optional[str] = None
    resource_type: Optional[str] = None

    async def fetch(self, manifest: "Manifest") -> None:
        data = await load_yaml_url(self.url)
        if self.scope is not None:
            self.scopes.append(self.scope)

        for ds in data["datasets"]:
            if self.scope is not None:
                ds["load"] = ds["name"] in self.scopes
            if self.namespace is not None:
                ds["namespace"] = self.namespace
            if self.resource_name is not None:
                ds["resource_name"] = self.resource_name
            if self.resource_type is not None:
                ds["resource_type"] = self.resource_type
            manifest.datasets.append(ds)


class Manifest(BaseModel):
    catalogs: List[CatalogManifest] = []
    datasets: List[Dict[str, Any]] = []

    @classmethod
    async def load(cls) -> "Manifest":
        data = await load_yaml_url(settings.MANIFEST)
        manifest = cls.model_validate(data)
        for catalog in manifest.catalogs:
            await catalog.fetch(manifest)
        # TODO: load remote metadata from a `metadata_url` on each dataset?
        return manifest


class Catalog(DataCatalog[Dataset]):
    """A collection of datasets, loaded from a manifest."""

    instance: Optional["Catalog"] = None

    @classmethod
    async def load(cls) -> "Catalog":
        manifest = await Manifest.load()
        catalog = cls(Dataset, {})
        for dmf in manifest.datasets:
            catalog.make_dataset(dmf)

        return catalog
