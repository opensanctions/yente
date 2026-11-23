import os.path
from typing import Annotated, Any, cast

from followthemoney.dataset import (
    DataCatalog,
    DatasetQuery,
    evaluate_query,
    parse_query,
    validate_query,
)
from followthemoney.exc import InvalidDatasetQuery
from pydantic import AfterValidator, BaseModel, ConfigDict, field_validator

from yente.data.common import ScoringConfigValues, ScoringWeights
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
    auth_token: str | None = None
    scope: str | None = None
    scopes: list[str] = []
    namespace: bool | None = None
    resource_name: str | None = None
    resource_type: str | None = None

    @field_validator("auth_token")
    @classmethod
    def expand_auth_token(cls, v: str | None) -> str | None:
        """Expand environment variables in the auth_token."""
        if v is not None:
            return os.path.expandvars(v)
        return v

    async def fetch_datasets(self) -> list[dict[str, Any]]:
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

        return cast(list[dict[str, Any]], data["datasets"])


class MatchProfile(BaseModel):
    """Named defaults for the `matcher.weights` and `matcher.config` fields of a
    match request."""

    model_config = ConfigDict(extra="forbid")

    weights: ScoringWeights = {}
    config: ScoringConfigValues = {}


def parse_dataset_query(value: DatasetQuery) -> DatasetQuery:
    """Parse a dataset query given as a string, and check a query is well-formed.

    Raises InvalidDatasetQuery. In the parsed form, a string is a single dataset
    name or tag.
    """
    query = parse_query(value) if isinstance(value, str) else value
    validate_query(query)
    return query


def _parse_manifest_query(value: DatasetQuery) -> DatasetQuery:
    try:
        return parse_dataset_query(value)
    except InvalidDatasetQuery as exc:
        raise ValueError(f"Invalid dataset query: {exc}") from exc


# A dataset query in the manifest, given as a string or in its structured form,
# and stored in its parsed form. Whether the datasets it names exist is checked
# against the catalog when it's used.
ManifestDatasetQuery = Annotated[DatasetQuery, AfterValidator(_parse_manifest_query)]


class FilterProfile(BaseModel):
    """Named defaults for the `filters.datasets` and `filters.topics` fields of a
    match request."""

    model_config = ConfigDict(extra="forbid")

    datasets: ManifestDatasetQuery | None = None
    topics: list[str] | None = None


class Manifest(BaseModel):
    """A manifest (usually loaded from a YAML configuration file) that specifies datasets
    to be loaded, directly and via catalogs."""

    catalogs: list[CatalogManifest] = []
    datasets: list[dict[str, Any]] = []
    # Default `None` here while /v2/match is in beta so that instances which don't
    # load the OpenSanctions `default` collection keep working without config changes.
    # /v2/match falls back to `default` in the router.
    default_datasets: ManifestDatasetQuery | None = None
    match_profiles: dict[str, MatchProfile] = {}
    filter_profiles: dict[str, FilterProfile] = {}

    @classmethod
    async def load(cls, manifest_path: str) -> "Manifest":
        """Load a manifest from the given path or URL."""
        data = await load_yaml_url(manifest_path)
        return cls.model_validate(data)

    async def fetch_datasets(self) -> list[dict[str, Any]]:
        """Fetch all datasets specified in the manifest."""

        all_datasets: list[dict[str, Any]] = []
        all_datasets.extend(self.datasets)
        for catalog in self.catalogs:
            all_datasets.extend(await catalog.fetch_datasets())
        # TODO: load remote metadata from a `metadata_url` on each dataset?
        return all_datasets


class Catalog(DataCatalog[Dataset]):
    """A collection of datasets, loaded from a manifest."""

    @classmethod
    async def load(cls, manifest: Manifest) -> "Catalog":
        catalog = cls(Dataset, {})

        # Populate the internal catalog from all datasets/catalogs specified in the manifest.
        for dataset_spec in await manifest.fetch_datasets():
            dataset_name = dataset_spec.get("name")
            if dataset_name is not None and catalog.get(dataset_name) is not None:
                raise YenteConfigError(f"Duplicate dataset name: {dataset_name!r}")
            catalog.make_dataset(dataset_spec)

        return catalog


def check_dataset_queries(manifest: Manifest, catalog: Catalog) -> None:
    """Check that the manifest's dataset queries only name datasets in the
    catalog loaded from it. Raises YenteConfigError."""
    queries: dict[str, DatasetQuery] = {}
    if manifest.default_datasets is not None:
        queries["default_datasets"] = manifest.default_datasets
    for name, profile in manifest.filter_profiles.items():
        if profile.datasets is not None:
            queries[f"filter_profiles.{name}.datasets"] = profile.datasets
    for path, query in queries.items():
        try:
            evaluate_query(catalog, query)
        except InvalidDatasetQuery as exc:
            raise YenteConfigError(f"Invalid {path} in the manifest: {exc}") from exc
