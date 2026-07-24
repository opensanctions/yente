from datetime import datetime, UTC

from opentelemetry import metrics

from yente import settings
from yente.logs import get_logger
from yente.provider.base import SearchProvider
from yente.search.versions import parse_index_name

log = get_logger(__name__)

_meter = metrics.get_meter("yente.data")
_indexed_dataset_version_time = _meter.create_gauge(
    "yente.data.indexed_dataset_version_time",
    unit="s",
    description="Unix timestamp of the dataset's last_export for each indexed dataset",
)


async def update_dataset_version_metric(
    dataset_name: str,
    index: str,
    provider: SearchProvider,
) -> None:
    metadata = await provider.get_index_metadata(index)
    last_export = metadata.get("last_export")
    if last_export is None:
        log.warning(
            "No last_export in index metadata", dataset=dataset_name, index=index
        )
        return
    try:
        dt = datetime.fromisoformat(last_export)
    except (TypeError, ValueError):
        log.warning(
            "Could not parse last_export from index metadata",
            dataset=dataset_name,
            index=index,
            last_export=last_export,
        )
        return
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    timestamp_s = int(dt.timestamp())
    _indexed_dataset_version_time.set(timestamp_s, {"dataset": dataset_name})


async def update_metrics(provider: SearchProvider) -> None:
    """Refresh all dataset-level metrics by reading them from each currently
    aliased index. Run on a cron so the gauges stay populated independently of
    the reindex flow (which only fires when AUTO_REINDEX is on and only updates
    the metric for the dataset it just reindexed)."""
    for aliased_index in await provider.get_alias_indices(settings.ENTITY_INDEX):
        try:
            index_info = parse_index_name(aliased_index)
        except ValueError:
            log.warn(f"Invalid index name: {aliased_index}")
            continue
        await update_dataset_version_metric(
            index_info.dataset_name, aliased_index, provider
        )
