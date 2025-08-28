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
def system_version() -> str:
    """Get the current version of the system."""
    parts = [v.rjust(2, "0") for v in followthemoney.__version__.split(".")]
    ftm_version = "".join(parts)[:6]
    return f"{settings.INDEX_VERSION}{ftm_version}-"


def parse_index_name(index: str, match_system_version: bool = False) -> IndexInfo:
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

    sys_version = system_version()
    # FIXME: We don't want to verify this, since old indexes can still be around and need
    # to be handled. The side effects of disabling this check seem positive, but we should
    # verify this.
    if match_system_version and not index_version.startswith(sys_version):
        raise ValueError("Index version does not start with the correct prefix.")

    # TODO(Leon Handreke): if len(system_version()) ever changes, this will break!
    dataset_version = index_version[len(sys_version) :]
    if len(dataset_version) < 1:
        raise ValueError("Index version must be at least one character long.")
    return IndexInfo(dataset, dataset_version, sys_version)


def construct_index_name(dataset: str, version: str | None = None) -> str:
    """
    Given a dataset and optionally a version construct a properly versioned index name.
    """
    if len(str(dataset)) < 1:
        raise ValueError("Dataset name must be at least one character long.")
    base = f"{settings.ENTITY_INDEX}-{dataset}"
    if version is None:
        return base
    return f"{base}-{construct_index_version(version)}"


def construct_index_version(version: str) -> str:
    """Given a version ID, return a version string with the version prefix."""
    if len(version) < 1:
        raise ValueError("Version must be at least one character long.")
    sys_version = system_version()
    combined = slugify(f"{sys_version}{version}", "-")
    if combined is None or len(combined) < len(sys_version) + 1:
        raise ValueError("Invalid version: %s%s." % (sys_version, version))
    return combined
