from pydantic import BaseModel, Field

from yente.data.common import (
    EntityExample,
    ScoredEntityResponse,
    ScoringConfigValues,
    ScoringWeights,
    TotalSpec,
)


class EntityMatchQuery(BaseModel):
    weights: ScoringWeights = Field({}, examples=[{"name_literal": 0.8}])
    config: ScoringConfigValues = Field(
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
