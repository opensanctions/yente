from datetime import datetime
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, Field
from nomenklatura.matching.types import MatchingResult, FeatureDocs

from yente.data.entity import Entity

EntityProperties = Dict[str, List[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., examples=["Detailed error message"])


class EntityResponse(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["John Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    properties: EntityProperties = Field(..., examples=[{"name": ["John Doe"]}])
    datasets: List[str] = Field([], examples=[["us_ofac_sdn"]])
    referents: List[str] = Field([], examples=[["ofac-1234"]])
    target: bool = Field(False)
    first_seen: Optional[datetime] = Field(None, examples=[datetime.utcnow()])
    last_seen: Optional[datetime] = Field(None, examples=[datetime.utcnow()])
    last_change: Optional[datetime] = Field(None, examples=[datetime.utcnow()])

    @classmethod
    def from_entity(cls, entity: Entity) -> "EntityResponse":
        return cls.model_validate(entity.to_dict())


EntityResponse.model_rebuild()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    features: Dict[str, float]
    match: bool = False

    @classmethod
    def from_entity_result(
        cls, entity: Entity, result: MatchingResult, threshold: float
    ) -> "ScoredEntityResponse":
        data = entity.to_dict()
        data["score"] = result.score
        data["features"] = result.features
        data["match"] = result.score >= threshold
        return cls.model_validate(data)


class StatusResponse(BaseModel):
    status: str = "ok"


class SearchFacetItem(BaseModel):
    name: str = Field(..., examples=["ru"])
    label: str = Field(..., examples=["Russia"])
    count: int = Field(1, examples=[42])


class SearchFacet(BaseModel):
    label: str = Field(..., examples=["Countries"])
    values: List[SearchFacetItem]


class TotalSpec(BaseModel):
    value: int = Field(..., examples=[42])
    relation: str = Field("eq", examples=["eq"])


class ResultsResponse(BaseModel):
    limit: int = Field(..., examples=[20])
    offset: int = Field(0, examples=[0])
    total: TotalSpec


class SearchResponse(ResultsResponse):
    results: List[EntityResponse]
    facets: Dict[str, SearchFacet]


class EntityExample(BaseModel):
    id: Optional[str] = Field(None, examples=["my-entity-id"])
    schema_: str = Field(..., examples=["Person"], alias="schema")
    properties: Dict[str, Union[str, List[str]]] = Field(
        ..., examples=[{"name": ["John Doe"]}]
    )


class EntityMatchQuery(BaseModel):
    weights: Dict[str, float] = Field({}, examples=[{"name_literal": 0.8}])
    queries: Dict[str, EntityExample]


class EntityMatches(BaseModel):
    status: int = Field(200, examples=[200])
    results: List[ScoredEntityResponse]
    total: TotalSpec
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: Dict[str, EntityMatches]
    matcher: FeatureDocs
    limit: int = Field(..., examples=[5])


class DatasetModel(BaseModel):
    name: str
    title: str
    summary: Optional[str] = None
    url: Optional[str] = None
    load: bool
    entities_url: Optional[str] = None
    version: str
    index_version: Optional[str] = None
    index_current: bool = False
    children: List[str]


class DataCatalogModel(BaseModel):
    datasets: List[DatasetModel]
    current: List[str]
    outdated: List[str]
    index_stale: bool = False


class Algorithm(BaseModel):
    name: str
    description: Optional[str] = None
    features: FeatureDocs


class AlgorithmResponse(BaseModel):
    algorithms: List[Algorithm]
    default: str
    best: str
