from datetime import datetime, timezone

from opentelemetry import metrics

from yente.logs import get_logger
from yente.provider.base import SearchProvider

log = get_logger(__name__)

_meter = metrics.get_meter("yente.data")
# Ideally we'd be reading dataset last_export from the index, but once a new index
# has been loaded, that information is no longer available to us — so we derive it
# by max-querying last_seen across all documents in the index.
_indexed_dataset_version_time = _meter.create_gauge(
    "indexed_dataset_version_time",
    unit="s",
    description="Unix timestamp of the most recent last_seen value for each indexed dataset",
)


async def update_dataset_version_metric(
    dataset_name: str,
    index: str,
    provider: SearchProvider,
) -> None:
    timestamp_s = await provider.get_index_max_date(index, "last_seen")
    if timestamp_s is None:
        log.warning(
            "No last_seen value found in index", dataset=dataset_name, index=index
        )
        return
    _indexed_dataset_version_time.set(timestamp_s, {"dataset": dataset_name})
    log.info(
        "Updated dataset version metric",
        dataset=dataset_name,
        timestamp=datetime.fromtimestamp(timestamp_s, tz=timezone.utc).isoformat(),
    )
