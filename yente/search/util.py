from yente import settings

from typing import Tuple


def parse_index_name(index: str) -> Tuple[str, str]:
    """
    Parse a given index name.

    Returns:
        dataset_name: str   The name of the dataset the index is based on
        version: str        The version of the index
    """
    # TODO: If we assert that no dashes are allowed in index names we can remove this check.
    if not index.startswith(settings.ENTITY_INDEX):
        raise ValueError("Index created with a different prefix and cannot be parsed.")
    index_end = index[len(settings.ENTITY_INDEX) + 1 :]
    if "-" not in index_end:
        raise ValueError("Index name does not contain a version.")
    dataset, index_version = index_end.split("-", 1)
    if not index_version.startswith(settings.INDEX_VERSION):
        raise ValueError("Index version does not start with the correct prefix.")
    dataset_version = index_version[len(settings.INDEX_VERSION) :]
    if len(dataset_version) < 1:
        raise ValueError("Index version must be at least one character long.")
    return (dataset, dataset_version)


def construct_index_name(ds_name: str, ds_version: str | None = None) -> str:
    """
    Given a dataset and optionally a version construct a properly versioned index name.
    """
    if len(str(ds_name)) < 1:
        raise ValueError("Dataset name must be at least one character long.")
    base = f"{settings.ENTITY_INDEX}-{ds_name}"
    if ds_version is None:
        return base
    return f"{base}-{construct_index_version(ds_version)}"


def construct_index_version(version: str) -> str:
    """Given a version ID, return a version string with the version prefix."""
    if len(version) < 1:
        raise ValueError("Version must be at least one character long.")
    return f"{settings.INDEX_VERSION}{version}"
