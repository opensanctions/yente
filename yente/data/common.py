from datetime import datetime
from typing import Any, Dict, List, Union, Optional
from pydantic import BaseModel, Field
from nomenklatura.matching.types import (
    MatchingResult,
    FeatureDocs,
    AlgorithmDocs,
    FtResult,
)

from yente import settings
from yente.data.dataset import YenteDatasetModel
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
    first_seen: Optional[datetime] = Field(None, examples=[settings.RUN_DT])
    last_seen: Optional[datetime] = Field(None, examples=[settings.RUN_DT])
    last_change: Optional[datetime] = Field(None, examples=[settings.RUN_DT])

    @classmethod
    def from_entity(cls, entity: Entity) -> "EntityResponse":
        return cls.model_validate(entity.to_dict())


EntityResponse.model_rebuild()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    features: Dict[str, float] = Field(
        description="A dictionary of subscores from features in the algorithm. Deprecated, use `explanations` instead.",
        deprecated=True,
    )
    explanations: Dict[str, FtResult] = Field(
        description="A dictionary of subscores from features in the algorithm and explanations for how they were calculated."
    )
    match: bool = Field(description="Whether the score is above the match threshold.")
    token: Optional[str] = None

    @classmethod
    def from_entity_result(
        cls, entity: Entity, result: MatchingResult, threshold: float
    ) -> "ScoredEntityResponse":
        data = entity.to_dict()
        data["score"] = result.score
        data["explanations"] = result.explanations
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


class AdjacentResultsResponse(ResultsResponse):
    results: List[Union[str, EntityResponse]] = Field(
        [],
        examples=[
            [
                {
                    "id": "ds-abc-own-123",
                    "schema": "Ownership",
                    "properties": {
                        "asset": [
                            {
                                "id": "NK-1234",
                                "caption": "Company 1 Ltd.",
                                "schema": "Comany",
                                "properties": {"name": ["Company 1 Ltd."]},
                            }
                        ]
                    },
                }
            ]
        ],
    )


class EntityAdjacentResponse(BaseModel):
    entity: EntityResponse
    adjacent: Dict[str, AdjacentResultsResponse]


class EntityExample(BaseModel):
    id: Optional[str] = Field(None, examples=["my-entity-id"])
    schema_: str = Field(..., examples=["Person"], alias="schema")
    properties: Dict[str, Union[str, List[Any]]] = Field(
        ..., examples=[{"name": ["John Doe"]}]
    )


class EntityMatchQuery(BaseModel):
    weights: Dict[str, float] = Field({}, examples=[{"name_literal": 0.8}])
    config: Dict[str, Union[str, int, float, bool, None]] = Field(
        default_factory=dict,
        description="Algorithm-specific configuration parameters.",
        examples=[{"nm_number_mismatch": 0.4}],
    )
    queries: Dict[str, EntityExample]


class EntityMatches(BaseModel):
    status: int = Field(200, examples=[200])
    results: List[ScoredEntityResponse]
    total: TotalSpec
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: Dict[str, EntityMatches]
    limit: int = Field(..., examples=[5])


class DataCatalogModel(BaseModel):
    datasets: List[YenteDatasetModel]
    current: List[str]
    outdated: List[str]
    index_stale: bool = False


class Algorithm(BaseModel):
    name: str
    description: Optional[str] = None
    features: FeatureDocs = Field(
        deprecated=True, description="Deprecated, use `docs` instead"
    )
    docs: AlgorithmDocs


class AlgorithmResponse(BaseModel):
    algorithms: List[Algorithm]
    default: str
    best: str
