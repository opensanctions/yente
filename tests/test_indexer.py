from datetime import datetime

import pytest
import pytest_asyncio

from yente import settings
from yente.data import get_catalog
from yente.data.manifest import Manifest
from yente.data.metrics import update_dataset_version_metric
from yente.provider import SearchProvider
from yente.search.indexer import update_index
from yente.search.mapping import INDEX_SETTINGS, make_entity_mapping
from yente.search.versions import build_index_name

from tests.conftest import (
    ZALA_MANIFEST,
    build_index_alias_name_for_fixture,
    metric_reader,
    patch_yente_catalog,
)


INITIAL_LAST_EXPORT_ISO = "2026-05-15T12:34:56+00:00"
UPDATED_LAST_EXPORT_ISO = "2026-06-10T08:00:00+00:00"


def _zala_manifest_with_last_export(version: str, last_export_iso: str) -> Manifest:
    # Pin `version` explicitly: otherwise yente derives it from the local
    # entities file's mtime, which doesn't change with `last_export`.
    data = ZALA_MANIFEST.model_dump(mode="json")
    data["datasets"][0]["version"] = version
    data["datasets"][0]["last_export"] = last_export_iso
    return Manifest.model_validate(data)


ZALA_WITH_INITIAL_LAST_EXPORT_MANIFEST = _zala_manifest_with_last_export(
    "v1", INITIAL_LAST_EXPORT_ISO
)
ZALA_WITH_UPDATED_LAST_EXPORT_MANIFEST = _zala_manifest_with_last_export(
    "v2", UPDATED_LAST_EXPORT_ISO
)


@pytest_asyncio.fixture(scope="function")
async def zala_with_last_export(monkeypatch):
    monkeypatch.setattr(
        settings,
        "ENTITY_INDEX",
        build_index_alias_name_for_fixture("zala-last-export"),
    )
    async with patch_yente_catalog(ZALA_WITH_INITIAL_LAST_EXPORT_MANIFEST):
        await update_index()
        yield


def _gauge_values_by_dataset(metric_name: str) -> dict[str, float]:
    """Return a {dataset -> value} dict of the most recent values for the named
    gauge across all `dataset` attribute labels."""
    result: dict[str, float] = {}
    data = metric_reader.get_metrics_data()
    if data is None:
        return result
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name != metric_name:
                    continue
                for point in metric.data.data_points:
                    dataset = point.attributes.get("dataset")
                    if dataset is not None:
                        result[dataset] = point.value
    return result


@pytest.mark.asyncio
async def test_index_entities_writes_last_export_to_metadata(
    zala_with_last_export,
    search_provider: SearchProvider,
):
    """index_entities should write dataset.model.last_export into the index
    `_meta` so it survives the load and can be read back."""
    catalog = await get_catalog()
    dataset = catalog.require("zala")
    assert dataset.model.version is not None
    index = build_index_name(dataset.name, dataset.model.version)
    metadata = await search_provider.get_index_metadata(index)
    assert metadata.get("last_export") == dataset.model.last_export.isoformat()


@pytest.mark.asyncio
async def test_reindex_with_new_last_export_updates_gauge(
    zala_with_last_export,
    search_provider: SearchProvider,
):
    """After re-loading the catalog with a newer `last_export`, the indexer
    creates a fresh index (the version is derived from `last_export`) and the
    gauge moves to the new timestamp."""
    # First load (via the fixture) — gauge should reflect the initial export.
    initial_values = _gauge_values_by_dataset("yente.data.indexed_dataset_version_time")
    assert initial_values.get("zala") == int(
        datetime.fromisoformat(INITIAL_LAST_EXPORT_ISO).timestamp()
    )

    # Re-load the catalog with a bumped last_export and re-index.
    async with patch_yente_catalog(ZALA_WITH_UPDATED_LAST_EXPORT_MANIFEST):
        await update_index()

    updated_values = _gauge_values_by_dataset("yente.data.indexed_dataset_version_time")
    assert updated_values.get("zala") == int(
        datetime.fromisoformat(UPDATED_LAST_EXPORT_ISO).timestamp()
    )


@pytest.mark.asyncio
async def test_update_dataset_version_metric_missing_metadata(
    search_provider: SearchProvider,
):
    """When the index has no `last_export` in `_meta`, no gauge value should be
    emitted for the dataset (the function logs a warning and returns)."""
    temp_index = settings.INDEX_NAME + "-metric-missing-meta"
    await search_provider.create_index(
        temp_index, mappings=make_entity_mapping(), settings=INDEX_SETTINGS
    )
    try:
        await update_dataset_version_metric(
            "dataset-with-no-meta", temp_index, search_provider
        )
        values = _gauge_values_by_dataset("yente.data.indexed_dataset_version_time")
        assert "dataset-with-no-meta" not in values
    finally:
        await search_provider.delete_index(temp_index)
