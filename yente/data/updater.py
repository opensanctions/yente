from typing import Optional, TypedDict, Dict, List, Any
from typing import AsyncGenerator, Tuple

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.loader import load_json_url, load_json_lines

log = get_logger(__name__)


class DeltaIndex(TypedDict):
    versions: Dict[str, str]


class EntityOp(TypedDict):
    op: str
    entity: Dict[str, Any]


class DatasetUpdater(object):
    """A helper object for emitting entity operations to transition from one
    loaded dataset version to the next."""

    def __init__(
        self, dataset: Dataset, base_version: Optional[str], force_full: bool = False
    ) -> None:
        self.dataset = dataset
        self.target_version = dataset.version or "static"
        self.base_version = base_version
        self.force_full = force_full
        self.delta_urls: Optional[List[Tuple[str, str]]] = None

    @classmethod
    async def build(
        cls, dataset: Dataset, base_version: Optional[str], force_full: bool = False
    ) -> "DatasetUpdater":
        """Fetch the index of delta files and decide an index building strategy."""
        obj = DatasetUpdater(dataset, base_version, force_full=force_full)
        if force_full:
            return obj
        if dataset.delta_url is None:
            log.debug("No delta updates available for: %r" % dataset.name)
            return obj
        if not settings.DELTA_UPDATES:
            return obj
        if obj.base_version is None or obj.target_version <= obj.base_version:
            return obj

        index: DeltaIndex = await load_json_url(dataset.delta_url)
        versions = index.get("versions", {})
        sorted_versions = sorted(versions.keys())
        if len(sorted_versions) == 0:
            return obj
        # We initially checked if the base_version was in the sorted_versions,
        # but the base_version can be a version that doesn't have a delta (no changes)
        # so we need to check if the base_version is older than the oldest delta version.
        if obj.base_version < min(sorted_versions):
            log.warning(
                "Loaded version of dataset is older than delta window",
                dataset=dataset.name,
                delta_url=dataset.delta_url,
                base_version=obj.base_version,
                target_version=obj.target_version,
                delta_versions=sorted_versions,
            )
            return obj

        obj.delta_urls = []
        for version in sorted_versions:
            if version <= obj.base_version or version > obj.target_version:
                continue
            obj.delta_urls.append((version, versions[version]))

        obj.target_version = max(sorted_versions)
        return obj

    @property
    def is_incremental(self) -> bool:
        """Check if there is sequence of delta entity patches that can be loaded."""
        if self.force_full:
            return False
        if not settings.DELTA_UPDATES:
            return False
        return self.delta_urls is not None

    def needs_update(self) -> bool:
        """Confirm that the dataset needs to be loaded."""
        if not self.dataset.load:
            return False
        if self.dataset.entities_url is None:
            log.warning(
                "Cannot identify resource with FtM entities",
                dataset=self.dataset.name,
            )
            return False
        if self.force_full:
            return True
        if self.target_version is None:
            raise False
        if self.delta_urls is not None and len(self.delta_urls) == 0:
            return False
        if self.base_version is not None and self.target_version <= self.base_version:
            return False
        return True

    async def load(self) -> AsyncGenerator[EntityOp, None]:
        """Generate entity change operations, including payload data."""
        if self.force_full or self.delta_urls is None:
            if self.dataset.entities_url is None:
                raise RuntimeError("No entities for dataset: %s" % self.dataset.name)
            base_name = f"{self.dataset.name}-{self.target_version}"
            async for data in load_json_lines(self.dataset.entities_url, base_name):
                yield {"op": "ADD", "entity": data}
            return

        for version, url in self.delta_urls:
            base_name = f"{self.dataset.name}-delta-{version}"
            async for data in load_json_lines(url, base_name):
                yield data
