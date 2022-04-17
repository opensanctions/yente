from datetime import datetime
from typing import Dict, List, Union
from pydantic import BaseModel, Field
from nomenklatura.matching.types import FeatureDocs
from nomenklatura.matching.types import MatchingResult

from yente import settings
from yente.data.entity import Entity

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

    @classmethod
    def from_entity(cls, entity: Entity):
        return cls(
            id=entity.id,
            caption=entity.caption,
            schema=entity.schema.name,
            properties=entity.properties,
            datasets=[ds.name for ds in entity.datasets],
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
    ):
        return cls(
            id=entity.id,
            caption=entity.caption,
            schema=entity.schema.name,
            properties=entity.properties,
            datasets=[ds.name for ds in entity.datasets],
            referents=list(entity.referents),
            target=entity.target,
            first_seen=entity.first_seen,
            last_seen=entity.last_seen,
            score=result["score"],
            match=result["score"] >= threshold,
            features=result["features"],
        )


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
