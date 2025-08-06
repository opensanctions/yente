from datetime import datetime
from typing import Dict, Optional, Any
from pydantic import Field, FilePath, computed_field, field_validator
from rigour.time import datetime_iso
from nomenklatura.util import iso_to_version
from followthemoney.dataset import Dataset as FollowTheMoneyDataset
from followthemoney.dataset.dataset import DatasetModel
from followthemoney.dataset.util import Url
from followthemoney.namespace import Namespace

from yente import settings
from yente.logs import get_logger
from yente.data.util import get_url_local_path

log = get_logger(__name__)


class YenteDatasetModel(DatasetModel):
    load: Optional[bool] = None
    entities_url: Optional[str] = None
    path: Optional[FilePath] = Field(None, exclude=True)
    auth_token: Optional[str] = Field(None, exclude=True)
    delta_url: Optional[Url] = None
    namespace: bool = False
    resource_name: Optional[str] = Field(None, exclude=True)
    resource_type: Optional[str] = Field(None, exclude=True)
    index_version: Optional[str] = None

    @field_validator("entities_url", mode="before")
    @classmethod
    def expand_entities_url(cls, value: Optional[str]) -> Optional[str]:
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
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.model: YenteDatasetModel = YenteDatasetModel.model_validate(data)

        if self.model.load is None:
            self.model.load = not self.is_collection
        if self.model.entities_url is None:
            self.model.entities_url = self._get_entities_url(data)

        if self.model.version is None:
            ts: Optional[str] = data.get("last_export", datetime_iso(settings.RUN_DT))
            if self.model.entities_url is not None:
                path = get_url_local_path(self.model.entities_url)
                if path is not None and path.exists():
                    mtime = path.stat().st_mtime
                    mdt = datetime.fromtimestamp(mtime)
                    ts = datetime_iso(mdt)
            if ts is not None:
                self.model.version = iso_to_version(ts) or "static"

        self.ns = Namespace(self.name) if self.model.namespace else None

    def _get_entities_url(self, data: Dict[str, Any]) -> Optional[str]:
        if self.model.path is not None:
            return self.model.path.resolve().as_uri()
        resource_name = self.model.resource_name
        resource_type = self.model.resource_type
        for resource in self.model.resources:
            if resource.url is None:
                continue
            if resource_name is not None and resource.name == resource_name:
                return resource.url
            if resource_type is not None and resource.mime_type == resource_type:
                return resource.url
        return None

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        if "children" not in data:
            data["children"] = [c.name for c in self.children]
        return data
