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
    return f"{settings.INDEX_VERSION}{ftm_version}"


def parse_index_name(index: str) -> IndexInfo:
    """
    Parse a given index name.

    Returns:
        IndexVersion: The parsed index version.
    Raises:
        ValueError: If the index name is not valid.
    """
    # TODO: If we assert that no dashes are allowed in index names we can remove this check.
    if not index.startswith(settings.ENTITY_INDEX):
        raise ValueError("Index created with a different prefix and cannot be parsed.")
    index_end = index[len(settings.ENTITY_INDEX) + 1 :]
    if "-" not in index_end:
        raise ValueError("Index name does not contain a version.")
    dataset, index_version = index_end.split("-", 1)

    # system_version must never contain a dash (asserted below when building),
    # dataset_version can contain dashes
    sys_version, dataset_version = index_version.split("-", 1)
    if len(dataset_version) < 1:
        raise ValueError("Index version must be at least one character long.")
    return IndexInfo(dataset, dataset_version, sys_version)


def build_index_name_prefix(dataset_name: str) -> str:
    if len(dataset_name) == 0:
        raise ValueError("Dataset name must be at least one character long.")
    # Assert this, otherwise our index parsing will break
    assert "-" not in dataset_name, "Dataset name must not contain a dash."
    return f"{settings.ENTITY_INDEX}-{dataset_name}"


def build_index_name(dataset_name: str, dataset_version: str) -> str:
    """
    Build an index name for a given dataset and dataset version.
    """
    if len(dataset_version) == 0:
        raise ValueError("Dataset version must be at least one character long.")

    # TODO(Leon Handreke): Do we really need the slugify here?
    sys_version = get_system_version()
    # Assert this, otherwise our index parsing will break
    assert "-" not in sys_version, "System version must not contain a dash."
    version = slugify(f"{get_system_version()}-{dataset_version}", "-")

    return f"{build_index_name_prefix(dataset_name)}-{version}"
