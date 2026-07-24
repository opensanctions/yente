from datetime import datetime
from typing import Any, Union
from pydantic import BaseModel, Field, field_serializer
from nomenklatura.matching.types import (
    MatchingResult,
    FeatureDocs,
    AlgorithmDocs,
    FeatureResult,
)

from yente import settings
from yente.data.dataset import YenteDatasetModel
from yente.data.entity import Entity

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., examples=["Detailed error message"])


class EntityResponse(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["John Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    properties: EntityProperties = Field(..., examples=[{"name": ["John Doe"]}])
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    referents: list[str] = Field([], examples=[["ofac-1234"]])
    target: bool = Field(False)
    first_seen: datetime | None = Field(None, examples=[settings.RUN_DT])
    last_seen: datetime | None = Field(None, examples=[settings.RUN_DT])
    last_change: datetime | None = Field(None, examples=[settings.RUN_DT])

    # Entities come out of ES with these already as ISO strings. Responses
    # are built via model_construct to skip re-validation, so pydantic has
    # a str where it expected a datetime — emit it as-is instead of warning.
    # A real datetime (should one ever appear) is still normalised to ISO.
    # The exact wire format is pinned by assert_iso_seconds_no_tz in tests.
    @field_serializer("first_seen", "last_seen", "last_change")
    def _serialize_datetime(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    @classmethod
    def from_entity(cls, entity: Entity) -> "EntityResponse":
        return cls.model_construct(**entity.to_dict())


EntityResponse.model_rebuild()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    explanations: dict[str, FeatureResult] = Field(
        description="A dictionary of subscores from features in the algorithm and explanations for how they were calculated."
    )
    match: bool = Field(description="Whether the score is above the match threshold.")

    @classmethod
    def from_entity_result(
        cls, entity: Entity, result: MatchingResult, threshold: float
    ) -> "ScoredEntityResponse":
        data = entity.to_dict()
        data["score"] = result.score
        data["explanations"] = result.explanations
        data["match"] = result.score >= threshold
        return cls.model_construct(**data)


class StatusResponse(BaseModel):
    status: str = "ok"


class SearchFacetItem(BaseModel):
    name: str = Field(..., examples=["ru"])
    label: str = Field(..., examples=["Russia"])
    count: int = Field(1, examples=[42])


class SearchFacet(BaseModel):
    label: str = Field(..., examples=["Countries"])
    values: list[SearchFacetItem]


class TotalSpec(BaseModel):
    value: int = Field(..., examples=[42])
    relation: str = Field("eq", examples=["eq"])


class ResultsResponse(BaseModel):
    limit: int = Field(..., examples=[20])
    offset: int = Field(0, examples=[0])
    total: TotalSpec


class SearchResponse(ResultsResponse):
    results: list[EntityResponse]
    facets: dict[str, SearchFacet]


class AdjacentResultsResponse(ResultsResponse):
    results: list[str | EntityResponse] = Field(
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
    adjacent: dict[str, AdjacentResultsResponse]


class EntityExample(BaseModel):
    id: str | None = Field(None, examples=["my-entity-id"])
    schema_: str = Field(..., examples=["Person"], alias="schema")
    properties: dict[str, str | list[Any]] = Field(
        ..., examples=[{"name": ["John Doe"]}]
    )


class EntityMatchQuery(BaseModel):
    weights: dict[str, float] = Field({}, examples=[{"name_literal": 0.8}])
    config: dict[str, str | int | float | bool | None] = Field(
        default_factory=dict,
        description="Algorithm-specific configuration parameters.",
        examples=[{"nm_number_mismatch": 0.4}],
    )
    queries: dict[str, EntityExample]


class EntityMatches(BaseModel):
    status: int = Field(200, examples=[200])
    results: list[ScoredEntityResponse]
    total: TotalSpec
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: dict[str, EntityMatches]
    limit: int = Field(..., examples=[5])


class DataCatalogModel(BaseModel):
    datasets: list[YenteDatasetModel]
    current: list[str]
    outdated: list[str]
    index_stale: bool = False


class Algorithm(BaseModel):
    name: str
    description: str | None = None
    features: FeatureDocs = Field(
        deprecated=True, description="Deprecated, use `docs` instead"
    )
    docs: AlgorithmDocs


class AlgorithmResponse(BaseModel):
    algorithms: list[Algorithm]
    default: str
    best: str
