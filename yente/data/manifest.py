import os.path
from typing import List, Optional, Dict, Any, cast

from followthemoney.dataset import DataCatalog
from pydantic import BaseModel, field_validator

from yente import settings
from yente.data.dataset import Dataset
from yente.data.loader import load_yaml_url
from yente.exc import YenteConfigError


class CatalogManifest(BaseModel):
    """A CatalogManifest specifies from where to load a catalog and which datasets to load from it.

    A catalog is a collection of datasets. The OpenSanctions catalog, for example, is available
    at https://data.opensanctions.org/datasets/latest/default/catalog.json and lists all datasets
    included in the default collection archive."""

    # The URL to load the catalog from.
    url: str
    # Token to set in the Authorization header. Also supports environment variable expansion.
    auth_token: Optional[str] = None
    scope: Optional[str] = None
    scopes: List[str] = []
    namespace: Optional[bool] = None
    resource_name: Optional[str] = None
    resource_type: Optional[str] = None

    @field_validator("auth_token")
    @classmethod
    def expand_auth_token(cls, v: Optional[str]) -> Optional[str]:
        """Expand environment variables in the auth_token."""
        if v is not None:
            return os.path.expandvars(v)
        return v

    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch the datasets from the catalog."""
        data = await load_yaml_url(self.url, auth_token=self.auth_token)
        if self.scope is not None:
            self.scopes.append(self.scope)

        invalid_scopes = set(self.scopes) - {ds["name"] for ds in data["datasets"]}
        if len(invalid_scopes):
            raise YenteConfigError(f"Scopes {invalid_scopes} not found in catalog")

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
    to be loaded, directly and via catalogs."""

    catalogs: List[CatalogManifest] = []
    datasets: List[Dict[str, Any]] = []

    @classmethod
    async def load(cls) -> "Manifest":
        """Load a manifest from the YAML file specified in the settings."""
        data = await load_yaml_url(settings.MANIFEST)
        return cls.model_validate(data)

    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch all datasets specified in the manifest."""

        all_datasets: List[Dict[str, Any]] = []
        all_datasets.extend(self.datasets)
        for catalog in self.catalogs:
            all_datasets.extend(await catalog.fetch_datasets())
        # TODO: load remote metadata from a `metadata_url` on each dataset?
        return all_datasets


class Catalog(DataCatalog[Dataset]):
    """A collection of datasets, loaded from a manifest."""

    instance: Optional["Catalog"] = None

    @classmethod
    async def load(cls, manifest: Optional[Manifest] = None) -> "Catalog":
        catalog = cls(Dataset, {})
        manifest = manifest or await Manifest.load()

        # Populate the internal catalog from all datasets/catalogs specified in the manifest.
        for dataset_spec in await manifest.fetch_datasets():
            dataset_name = dataset_spec.get("name")
            if dataset_name is not None and catalog.get(dataset_name) is not None:
                raise YenteConfigError(f"Duplicate dataset name: {dataset_name!r}")
            catalog.make_dataset(dataset_spec)

        return catalog
