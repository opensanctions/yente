from typing import List, Optional
from pydantic import BaseModel

from yente import settings
from yente.data.dataset import DatasetManifest
from yente.data.loader import load_yaml_url
from yente.data.statements import StatementManifest
from yente.data.util import iso_to_version


class ExternalManifest(BaseModel):
    """OpenSanctions is not one dataset but a whole collection, so this
    side-loads it into the yente dataset archive."""

    url: str
    type: str = "opensanctions"
    scope: str
    namespace: bool = False

    async def fetch(self, manifest: "Manifest") -> None:
        assert self.type == "opensanctions"
        data = await load_yaml_url(self.url)

        for ds in data["datasets"]:
            datasets = ds.get("sources", [])
            datasets.extend(ds.get("externals", []))
            dataset = DatasetManifest(
                name=ds["name"],
                title=ds["title"],
                version=iso_to_version(ds["last_export"]),
                namespace=self.namespace,
                collections=ds.get("collections", []),
                datasets=datasets,
            )
            if dataset.name == self.scope:
                for resource in ds["resources"]:
                    if resource["path"] == "entities.ftm.json":
                        dataset.url = resource["url"]
            manifest.datasets.append(dataset)

        stmt = StatementManifest(
            name=self.type,
            url=data["statements_url"],
            version=iso_to_version(data["run_time"]),
        )
        manifest.statements.append(stmt)


class Manifest(BaseModel):
    schedule: Optional[str] = None
    external: Optional[ExternalManifest] = None
    datasets: List[DatasetManifest] = []
    statements: List[StatementManifest] = []

    @classmethod
    async def load(cls) -> "Manifest":
        data = await load_yaml_url(settings.MANIFEST)
        manifest = cls.parse_obj(data)
        if manifest.external is not None:
            await manifest.external.fetch(manifest)
        return manifest
