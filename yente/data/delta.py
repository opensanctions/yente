from typing import Optional, TypedDict, Dict, List, Any
from typing import AsyncGenerator, Tuple

from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data.loader import load_json_url, load_json_lines

log = get_logger(__name__)


class DeltaIndex(TypedDict):
    versions: Dict[str, str]


class EntityOp(TypedDict):
    op: str
    entity: Dict[str, Any]


class DatasetLoader(object):
    """A helper object for emitting entity operations to transition from one
    loaded dataset version to the next."""

    def __init__(self, dataset: Dataset, base_version: Optional[str]) -> None:
        self.dataset = dataset
        self.target_version = dataset.version
        self.base_version = base_version
        self.is_prepared: bool = False
        self.delta_urls: Optional[List[Tuple[str, str]]] = None

    async def prepare(self) -> None:
        """Fetch the index of delta files and decide an index building strategy."""
        if self.is_prepared:
            return
        self.is_prepared = True
        if self.dataset.delta_url is None:
            log.debug("No delta updates available for: %r" % self.dataset.name)
            return
        if self.base_version is None or self.target_version <= self.base_version:
            return

        index: DeltaIndex = await load_json_url(self.dataset.delta_url)
        versions = index.get("versions", {})
        sorted_versions = sorted(versions.keys())
        if len(sorted_versions) == 0:
            return
        if self.base_version < min(sorted_versions):
            log.warning(
                "Loaded version of dataset is older than delta window",
                dataset=self.dataset.name,
                delta_url=self.dataset.delta_url,
                base_version=self.base_version,
                target_version=self.target_version,
            )
            return

        self.delta_urls = []
        for version in sorted_versions:
            if version <= self.base_version or version > self.target_version:
                continue
            self.delta_urls.append((version, versions[version]))

        # TODO: is this smart? this avoids running clones when there is not change:
        self.target_version = max(sorted_versions)

    @property
    def is_incremental(self) -> bool:
        """Check if there is sequence of delta entity patches that can be loaded."""
        if not self.is_prepared:
            raise RuntimeError(
                "Cannot call is_incremental before preparing the loader!"
            )
        return self.delta_urls is not None and len(self.delta_urls) > 0

    async def check(self, force_full: bool = False) -> bool:
        """Confirm that the dataset needs to be loaded."""
        await self.prepare()
        if not self.dataset.load:
            return False
        if self.dataset.entities_url is None:
            log.warning(
                "Cannot identify resource with FtM entities",
                dataset=self.dataset.name,
            )
            return False
        if force_full:
            return True
        if self.target_version == self.base_version:
            return False
        return True

    async def load(self, force_full: bool = False) -> AsyncGenerator[EntityOp, None]:
        """Generate entity change operations, including payload data."""
        await self.prepare()
        if force_full or self.delta_urls is None:
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
