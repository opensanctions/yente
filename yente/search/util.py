from yente.settings import ENTITY_INDEX, INDEX_VERSION

from typing import Tuple


def parse_index_name(index: str) -> Tuple[str, str]:
    """
    Parse a given index name.

    Returns:
        prefix: str         The configured index prefix
        dataset_name: str   The name of the dataset the index is based on
        version: str        The version of the index
    """
    # TODO: If we assert that no dashes are allowed in index names we can remove this check.
    if not index.startswith(ENTITY_INDEX):
        raise ValueError("Index created with a different prefix and cannot be parsed.")
    index_end = index[len(ENTITY_INDEX) + 1 :]
    dataset, index_version = index_end.split("-", 1)
    dataset_version = index_version[len(INDEX_VERSION) :]
    return (dataset, dataset_version)


def construct_index_name(ds_name: str, ds_version: str | None = None) -> str:
    """
    Given a dataset object and optionally a version construct a properly versioned index name.
    """
    if len(str(ds_name)) < 1:
        raise ValueError("Dataset name must be at least one character long.")
    base = f"{ENTITY_INDEX}-{ds_name}"
    if ds_version is None:
        return base
    return f"{base}-{construct_index_version(ds_version)}"


def construct_index_version(version: str) -> str:
    """
    Given a version string, return a version string with the version prefix.
    """
    if len(version) < 1:
        raise ValueError("Version must be at least one character long.")
    return f"{INDEX_VERSION}{version}"
