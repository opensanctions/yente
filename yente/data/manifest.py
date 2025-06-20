import itertools
from typing import List, Optional, Dict, Any, cast

from nomenklatura.dataset import DataCatalog
from pydantic import BaseModel

from yente import settings
from yente.data.dataset import Dataset
from yente.data.loader import load_yaml_url


class CatalogManifest(BaseModel):
    """A CatalogManifest specifies from where to load a catalog and which datasets to load from it.

    A catalog is a collection of datasets. The OpenSanctions catalog for example is available
    at https://data.opensanctions.org/datasets/latest/index.json and lists all datasets
    available in the OpenSanctions dataset archive."""

    # The URL to load the catalog from.
    url: str
    # The authentication token to use when loading the catalog and its datasets.
    auth_token: Optional[str] = None
    scope: Optional[str] = None
    scopes: List[str] = []
    namespace: Optional[bool] = None
    resource_name: Optional[str] = None
    resource_type: Optional[str] = None

    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch the datasets from the catalog."""
        data = await load_yaml_url(self.url, auth_token=self.auth_token)
        if self.scope is not None:
            self.scopes.append(self.scope)

        for ds in data["datasets"]:
            if len(self.scopes):
                ds["load"] = ds["name"] in self.scopes
            if self.namespace is not None:
                ds["namespace"] = self.namespace
            if self.resource_name is not None:
                ds["resource_name"] = self.resource_name
            if self.resource_type is not None:
                ds["resource_type"] = self.resource_type
            if self.auth_token is not None:
                ds["auth_token"] = self.auth_token

        return cast(List[Dict[str, Any]], data["datasets"])


class Manifest(BaseModel):
    """A manifest (usually loaded from a YAML configuration file) that specifies datasets
    to be loaded, either directly or via catalogs."""

    catalogs: List[CatalogManifest] = []
    datasets: List[Dict[str, Any]] = []

    @classmethod
    async def load(cls) -> "Manifest":
        """Load a manifest from the YAML file specified in the settings."""
        data = await load_yaml_url(settings.MANIFEST)
        return cls.model_validate(data)

    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch all datasets specified in the manifest."""

        # TODO: load remote metadata from a `metadata_url` on each dataset?
        return list(
            itertools.chain(
                self.datasets,
                *[await catalog.fetch_datasets() for catalog in self.catalogs],
            )
        )


class Catalog(DataCatalog[Dataset]):
    """A collection of datasets, loaded from a manifest."""

    instance: Optional["Catalog"] = None

    @classmethod
    async def load(cls, manifest: Optional[Manifest] = None) -> "Catalog":
        catalog = cls(Dataset, {})

        manifest = manifest or await Manifest.load()
        # Populate the internal catalog from all datasets/catalogs specified in the manifest.
        for dataset_spec in await manifest.fetch_datasets():
            catalog.make_dataset(dataset_spec)

        return catalog
