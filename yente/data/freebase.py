from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from pydantic.networks import AnyHttpUrl
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.property import Property

from yente import settings
from yente.data.common import ScoredEntityResponse


class FreebaseType(BaseModel):
    id: str = Field(..., examples=["Person"])
    name: str = Field(..., examples=["People"])
    description: Optional[str] = None

    @classmethod
    def from_schema(cls, schema: Schema) -> "FreebaseType":
        desc = schema.description or schema.label
        return cls(id=schema.name, name=schema.plural, description=desc)


class FreebaseProperty(BaseModel):
    id: str = Field(..., examples=["birthDate"])
    name: str = Field(..., examples=["Date of birth"])
    description: Optional[str] = None

    @classmethod
    def from_prop(cls, prop: Property) -> "FreebaseProperty":
        return cls(id=prop.qname, name=prop.label, description=prop.description)


class FreebaseEntity(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    name: str = Field(..., examples=["John Doe"])
    description: Optional[str] = None
    type: List[FreebaseType]

    @classmethod
    def from_proxy(cls, proxy: EntityProxy) -> "FreebaseEntity":
        type_ = [FreebaseType.from_schema(proxy.schema)]
        assert proxy.id is not None, "EntityProxy must have an ID"
        return FreebaseEntity(
            id=proxy.id,
            name=proxy.caption,
            type=type_,
            description=proxy.schema.label,
        )


class FreebaseScoredEntity(FreebaseEntity):
    score: Optional[float] = Field(..., examples=[0.99])
    match: Optional[bool] = Field(..., examples=[False])

    @classmethod
    def from_scored(cls, data: ScoredEntityResponse) -> "FreebaseScoredEntity":
        schema = model.get(data.schema_)
        if schema is None:
            raise RuntimeError("Missing schema: %s" % data.schema_)
        return cls(
            id=data.id,
            name=data.caption,
            description=schema.label,
            type=[FreebaseType.from_schema(schema)],
            score=data.score,
            match=data.match,
        )


class FreebaseResponse(BaseModel):
    code: str = "/api/status/ok"
    status: str = "200 OK"


class FreebaseSuggestResponse(FreebaseResponse):
    prefix: str


class FreebaseTypeSuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseType]


class FreebaseEntitySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseEntity]


class FreebasePropertySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseProperty]


class FreebaseExtendProperty(BaseModel):
    id: str
    name: str


class FreebaseExtendPropertiesResponse(BaseModel):
    limit: int
    type: str
    properties: List[FreebaseExtendProperty]


class FreebaseRenderMethod(str, Enum):
    raw = "raw"
    caption = "caption"


class FreebaseExtendQueryPropertySettings(BaseModel):
    limit: int = 0
    render: FreebaseRenderMethod = FreebaseRenderMethod.caption


class FreebaseExtendQueryProperty(BaseModel):
    id: str
    settings: FreebaseExtendQueryPropertySettings = (
        FreebaseExtendQueryPropertySettings()
    )


class FreebaseExtendQuery(BaseModel):
    ids: List[str]
    properties: List[FreebaseExtendQueryProperty]


class FreebaseExtendResponseMeta(BaseModel):
    id: str
    name: str


class FreebaseExtendResponseValue(BaseModel):
    str: str


class FreebaseExtendResponse(BaseModel):
    meta: List[FreebaseExtendResponseMeta]
    rows: Dict[str, Dict[str, List[FreebaseExtendResponseValue]]]


class FreebaseManifestView(BaseModel):
    url: str


class FreebaseManifestPreview(BaseModel):
    url: str
    width: int
    height: int


class FreebaseManifestSuggestType(BaseModel):
    service_url: AnyHttpUrl
    service_path: str


class FreebaseManifestSuggest(BaseModel):
    entity: FreebaseManifestSuggestType
    type: FreebaseManifestSuggestType
    property: FreebaseManifestSuggestType


class FreebaseManifestExtendProposeProperties(BaseModel):
    service_url: AnyHttpUrl
    service_path: str


class FreebaseManifestExtendPropertySettingChoice(BaseModel):
    id: str
    name: str


class FreebaseManifestExtendPropertySetting(BaseModel):
    name: str
    label: str
    type: str
    default: Any
    help_text: str
    choices: List[FreebaseManifestExtendPropertySettingChoice] = []


class FreebaseManifestExtend(BaseModel):
    propose_properties: FreebaseManifestExtendProposeProperties
    propose_settings: List[FreebaseManifestExtendPropertySetting]


class FreebaseManifest(BaseModel):
    versions: List[str] = Field(..., examples=[["0.2"]])
    name: str = Field(..., examples=[settings.TITLE])
    identifierSpace: AnyHttpUrl
    schemaSpace: AnyHttpUrl
    documentation: AnyHttpUrl
    batchSize: int
    view: FreebaseManifestView
    preview: FreebaseManifestPreview
    suggest: FreebaseManifestSuggest
    extend: FreebaseManifestExtend
    defaultTypes: List[FreebaseType]


class FreebaseEntityResult(BaseModel):
    result: List[FreebaseScoredEntity]
