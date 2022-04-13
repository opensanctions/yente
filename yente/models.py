from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field
from pydantic.networks import AnyHttpUrl
from nomenklatura.matching.types import FeatureDocs
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.property import Property

from yente import settings

EntityProperties = Dict[str, List[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str


class PartialErrorResponse(ErrorResponse):
    status: int = Field(..., example=400)


class EntityResponse(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    caption: str = Field(..., example="John Doe")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    properties: EntityProperties = Field(..., example={"name": ["John Doe"]})
    datasets: List[str] = Field([], example=["us_ofac_sdn"])
    referents: List[str] = Field([], example=["ofac-1234"])
    target: bool = Field(False)
    first_seen: datetime = Field(..., example=datetime.utcnow())
    last_seen: datetime = Field(..., example=datetime.utcnow())


EntityResponse.update_forward_refs()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    features: Dict[str, float]
    match: bool = False


class HealthzResponse(BaseModel):
    status: str = "ok"


class SearchFacetItem(BaseModel):
    name: str
    label: str
    count: int = 1


class SearchFacet(BaseModel):
    label: str
    values: List[SearchFacetItem]


class TotalSpec(BaseModel):
    value: int
    relation: str


class ResultsResponse(BaseModel):
    limit: int
    offset: int = 0
    total: TotalSpec


class SearchResponse(ResultsResponse):
    results: List[EntityResponse]
    facets: Dict[str, SearchFacet]


class EntityExample(BaseModel):
    schema_: str = Field(..., example=settings.BASE_SCHEMA, alias="schema")
    properties: Dict[str, Union[str, List[str]]] = Field(
        ..., example={"name": ["John Doe"]}
    )


class EntityMatchQuery(BaseModel):
    queries: Dict[str, EntityExample]


class EntityMatches(BaseModel):
    status: int = Field(200, example=200)
    results: List[ScoredEntityResponse]
    total: TotalSpec
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: Dict[str, Union[EntityMatches, PartialErrorResponse]]
    matcher: FeatureDocs
    limit: int


class StatementModel(BaseModel):
    id: str = Field(..., example="0000ad52d4d91a8...")
    entity_id: str = Field(..., example="ofac-1234")
    canonical_id: str = Field(..., example="NK-1234")
    prop: str = Field(..., example="alias")
    prop_type: str = Field(..., example="name")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    value: str = Field(..., example="John Doe")
    dataset: str = Field(..., example="default")
    target: bool = Field(..., example=True)
    first_seen: datetime
    last_seen: datetime


class StatementResponse(ResultsResponse):
    results: List[StatementModel]


class FreebaseType(BaseModel):
    id: str = Field(..., example="Person")
    name: str = Field(..., example="People")
    description: Optional[str] = Field(None, example="...")

    @classmethod
    def from_schema(cls, schema: Schema) -> "FreebaseType":
        desc = schema.description or schema.label
        return cls(id=schema.name, name=schema.plural, description=desc)


class FreebaseProperty(BaseModel):
    id: str = Field(..., example="birthDate")
    name: str = Field(..., example="Date of birth")
    description: Optional[str] = Field(None, example="...")

    @classmethod
    def from_prop(cls, prop: Property) -> "FreebaseProperty":
        return cls(id=prop.qname, name=prop.label, description=prop.description)


class FreebaseEntity(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    name: str = Field(..., example="John Doe")
    description: Optional[str] = Field(None, example="...")
    type: List[FreebaseType]

    @classmethod
    def from_proxy(cls, proxy: EntityProxy):
        type_ = [FreebaseType.from_schema(proxy.schema)]
        return FreebaseEntity(id=proxy.id, name=proxy.caption, type=type_)


class FreebaseScoredEntity(FreebaseEntity):
    score: Optional[float] = Field(..., example=0.99)
    match: Optional[bool] = Field(..., example=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "FreebaseScoredEntity":
        schema = model.get(data["schema"])
        if schema is None:
            raise RuntimeError("Missing schema: %s" % data["schema"])
        return cls(
            id=data["id"],
            name=data["caption"],
            type=[FreebaseType.from_schema(schema)],
            score=data["score"],
            match=data["match"],
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


class FreebaseManifest(BaseModel):
    versions: List[str] = Field(..., example=["0.2"])
    name: str = Field(..., example=settings.TITLE)
    identifierSpace: AnyHttpUrl
    schemaSpace: AnyHttpUrl
    view: FreebaseManifestView
    preview: FreebaseManifestPreview
    suggest: FreebaseManifestSuggest
    defaultTypes: List[FreebaseType]


class FreebaseEntityResult(BaseModel):
    result: List[FreebaseScoredEntity]


FreebaseQueryResult = Dict[str, FreebaseEntityResult]
