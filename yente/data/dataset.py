from datetime import datetime, UTC
from typing import Any
from pydantic import Field, FilePath, computed_field, field_validator
from rigour.time import datetime_iso
from followthemoney.dataset import DataResource, Dataset as FollowTheMoneyDataset
from followthemoney.dataset.dataset import DatasetModel
from followthemoney.dataset.util import Url
from followthemoney.namespace import Namespace

from yente import settings
from yente.logs import get_logger
from yente.data.util import get_url_local_path, iso_to_version

log = get_logger(__name__)


class YenteDatasetModel(DatasetModel):
    load: bool | None = None
    entities_url: str | None = None
    entities_checksum: str | None = Field(None, exclude=True)
    path: FilePath | None = Field(None, exclude=True)
    auth_token: str | None = Field(None, exclude=True)
    delta_url: Url | None = None
    namespace: bool = False
    resource_name: str | None = Field(None, exclude=True)
    resource_type: str | None = Field(None, exclude=True)
    index_version: str | None = None

    @field_validator("entities_url", mode="before")
    @classmethod
    def expand_entities_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        entities_path = get_url_local_path(value)
        if entities_path is not None:
            value = entities_path.as_uri()
        return value

    @computed_field  # type: ignore[prop-decorator]
    @property
    def index_current(self) -> bool:
        return self.index_version == self.version


class Dataset(FollowTheMoneyDataset):
    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        self.model: YenteDatasetModel = YenteDatasetModel.model_validate(data)

        if self.model.load is None:
            self.model.load = not self.is_collection
        if self.model.entities_url is None:
            if self.model.path is not None:
                self.model.entities_url = self.model.path.resolve().as_uri()
            else:
                resource = self._get_entities_resource(data)
                if resource is not None:
                    self.model.entities_url = resource.url
                    if settings.VERIFY_CHECKSUM:
                        self.model.entities_checksum = resource.checksum
                        # If checksum check is enabled and we're using a resource as a data source,
                        # warn if the resource does not have a checksum.
                        if self.model.entities_checksum is None:
                            log.warning(
                                "Resource %s does not have a checksum.", resource.url
                            )

        if self.model.version is None:
            ts: str | None = data.get("last_export", datetime_iso(settings.RUN_DT))
            if self.model.entities_url is not None:
                path = get_url_local_path(self.model.entities_url)
                if path is not None and path.exists():
                    mtime = path.stat().st_mtime
                    mdt = datetime.fromtimestamp(mtime, tz=UTC)
                    ts = datetime_iso(mdt)
            if ts is not None:
                self.model.version = iso_to_version(ts) or "static"

        self.ns = Namespace(self.name) if self.model.namespace else None

    def _get_entities_resource(self, data: dict[str, Any]) -> DataResource | None:
        """Return entities resource identified by catalog options."""
        resource_name = self.model.resource_name
        resource_type = self.model.resource_type
        for resource in self.model.resources:
            if resource.url is None:
                continue
            if resource_name is not None and resource.name == resource_name:
                return resource
            if resource_type is not None and resource.mime_type == resource_type:
                return resource
        return None

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        if "children" not in data:
            data["children"] = [c.name for c in self.children]
        return data
