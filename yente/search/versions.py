from dataclasses import dataclass
from functools import cache
from normality import slugify
import followthemoney

from yente import settings


@dataclass
class IndexInfo:
    dataset_name: str
    dataset_version: str
    system_version: str


@cache
def get_system_version() -> str:
    """Get the current version of the system."""
    parts = [v.rjust(2, "0") for v in followthemoney.__version__.split(".")]
    ftm_version = "".join(parts)[:6]
    return f"{settings.INDEX_VERSION}{settings.INDEX_REBUILD_ID}{ftm_version}"


def parse_index_name(index: str) -> IndexInfo:
    """
    Parse a given index name.

    An index name is of the form <settings.INDEX_NAME>-<system_version>-entities-<dataset_name>-<dataset_version>

    Returns:
        IndexVersion: The parsed index version.
    Raises:
        ValueError: If the index name is not valid.
    """
    alias = get_index_alias_name()
    # TODO: If we assert that no dashes are allowed in index names we can remove this check.
    if not index.startswith(alias):
        raise ValueError("Index created with a different prefix and cannot be parsed.")

    # len(settings.INDEX_NAME) + 1 because we also want to skip the dash
    remainder = index[len(settings.INDEX_NAME) + 1 :]

    # Note: this hinges on system_version and dataset_name not containing dashes
    # dataset_version can contain dashes
    system_version, index_type, dataset_name, dataset_version = remainder.split("-", 3)
    if index_type != "entities":
        raise ValueError("Index type must be entities.")

    return IndexInfo(dataset_name, dataset_version, system_version)


def build_index_name_prefix(dataset_name: str) -> str:
    if len(dataset_name) == 0:
        raise ValueError("Dataset name must be at least one character long.")
    # Assert this, otherwise our index parsing will break
    assert "-" not in dataset_name, "Dataset name must not contain a dash."
    return f"{get_index_alias_name()}-{dataset_name}"


def build_index_name(dataset_name: str, dataset_version: str) -> str:
    """
    Build an index name for a given dataset and dataset version.
    """
    if len(dataset_version) == 0:
        raise ValueError("Dataset version must be at least one character long.")

    # OpenSanctions datasets are usually 202501011200-abc, but slugify to make no assumptions
    dataset_version_slugified = slugify(dataset_version, "-")

    return f"{build_index_name_prefix(dataset_name)}-{dataset_version_slugified}"


def get_index_alias_name() -> str:
    # Assert this, otherwise our index parsing will break
    assert "-" not in get_system_version(), "System version must not contain a dash."

    return f"{settings.INDEX_NAME}-{get_system_version()}-entities"
