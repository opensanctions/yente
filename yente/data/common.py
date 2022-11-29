from datetime import datetime
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, Field
from nomenklatura.matching.types import FeatureDocs
from nomenklatura.matching.types import MatchingResult

from yente import settings
from yente.data.entity import Entity

EntityProperties = Dict[str, List[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., example="Detailed error message")


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

    @classmethod
    def from_entity(cls, entity: Entity) -> "EntityResponse":
        return cls(
            id=entity.id,
            caption=entity._caption,
            schema=entity.schema.name,
            properties=dict(entity.properties),
            datasets=list(entity.datasets),
            referents=list(entity.referents),
            target=entity.target,
            first_seen=entity.first_seen,
            last_seen=entity.last_seen,
        )


EntityResponse.update_forward_refs()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    features: Dict[str, float]
    match: bool = False

    @classmethod
    def from_entity_result(
        cls, entity: Entity, result: MatchingResult, threshold: float
    ) -> "ScoredEntityResponse":
        return cls(
            id=entity.id,
            caption=entity.caption,
            schema=entity.schema.name,
            properties=entity.properties,
            datasets=list(entity.datasets),
            referents=list(entity.referents),
            target=entity.target,
            first_seen=entity.first_seen,
            last_seen=entity.last_seen,
            score=result["score"],
            match=result["score"] >= threshold,
            features=result["features"],
        )


class StatusResponse(BaseModel):
    status: str = "ok"


class SearchFacetItem(BaseModel):
    name: str = Field(..., example="ru")
    label: str = Field(..., example="Russia")
    count: int = Field(1, example=42)


class SearchFacet(BaseModel):
    label: str = Field(..., example="Countries")
    values: List[SearchFacetItem]


class TotalSpec(BaseModel):
    value: int = Field(..., example=42)
    relation: str = Field("eq", example="eq")


class ResultsResponse(BaseModel):
    limit: int = Field(..., example=20)
    offset: int = Field(0, example=0)
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
    responses: Dict[str, EntityMatches]
    matcher: FeatureDocs
    limit: int = Field(..., example=5)


class DatasetModel(BaseModel):
    name: str
    title: str
    summary: Optional[str]
    url: Optional[str]
    load: bool
    entities_url: Optional[str]
    version: str
    children: List[str]


class DataCatalogModel(BaseModel):
    datasets: List[DatasetModel]
