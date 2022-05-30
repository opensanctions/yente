import yaml
from aiohttp import ClientSession
from typing import List, Optional
from pydantic import BaseModel, AnyHttpUrl

from yente import settings
from yente.data.dataset import DatasetManifest
from yente.data.statements import StatementManifest
from yente.data.loader import URL, http_timeout
from yente.data.util import iso_to_version


class ExternalManifest(BaseModel):
    """OpenSanctions is not one dataset but a whole collection, so this
    side-loads it into the yente dataset archive."""

    url: AnyHttpUrl
    type: str = "opensanctions"
    scope: str
    namespace: bool = False

    async def fetch(self, manifest: "Manifest") -> None:
        assert self.type == "opensanctions"
        async with ClientSession(timeout=http_timeout) as client:
            async with client.get(self.url) as resp:
                data = await resp.json()

        for ds in data["datasets"]:
            datasets = ds.get("sources", [])
            datasets.extend(ds.get("externals", []))
            dataset = DatasetManifest(
                name=ds["name"],
                title=ds["title"],
                version=iso_to_version(ds["last_export"]),
                namespace=self.namespace,
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
        with open(settings.MANIFEST, "r") as fh:
            data = yaml.safe_load(fh)
        manifest = cls.parse_obj(data)
        if manifest.external is not None:
            await manifest.external.fetch(manifest)
        return manifest
