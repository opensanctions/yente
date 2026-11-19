from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from yente import settings
from yente.data.common import ScoredEntityResponse, TotalSpec
from yente.routers.util import ALGO_HELP, TS_PATTERN


class QueryEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_: str = Field(min_length=1, alias="schema", examples=["Person"])
    properties: dict[str, str | list[Any]] = Field(
        min_length=1, examples=[{"name": ["John Doe"]}]
    )


class MatcherOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weights: dict[str, float] = Field(
        default={},
        description="Override the weights of features in the scoring algorithm.",
        examples=[{"name_literal": 0.8}],
    )
    config: dict[str, str | int | float | bool | None] = Field(
        default={},
        description="Algorithm-specific configuration parameters.",
        examples=[{"nm_number_mismatch": 0.4}],
    )


class Filters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The structured form of followthemoney.dataset.DatasetQuery. A top-level
    # string is excluded so it can't be confused with the string query syntax.
    datasets: list[Any] | dict[str, Any] = Field(
        default=["default"],
        description=(
            "The datasets to match against, as a structured FollowTheMoney dataset "
            "query: a list of dataset names, collection names and `#tag` selectors, "
            "or an `or`/`and`/`not` operator object. "
            "Defaults to the `default` collection. "
            "See https://followthemoney.tech/docs/metadata/#dataset-query-dsl"
        ),
        examples=[["#list.sanction"], {"and": ["default", {"not": "#list.pep"}]}],
    )
    topics: list[str] = Field(
        default=[],
        title="Only return results that match any of the given topics",
        examples=[["role.pep", "sanction"]],
    )
    changed_since: str | None = Field(
        default=None,
        pattern=TS_PATTERN,
        title="Match against entities that were updated since the given date",
        examples=["2026-01-01"],
    )
    exclude_entities: list[str] = Field(
        default=[],
        max_length=50,
        title="A list of entities IDs to exclude from matching",
        description=(
            "The entity IDs supplied here do not have to be canonical. Supplying any "
            "of the referents of a merged entity will exclude that entity. This "
            "parameter may be useful for example to exclude false-positive matches "
            "that have been decided upon by a human."
        ),
        examples=[["NK-abc123"]],
    )
    exclude_schemata: list[str] = Field(
        default=[],
        title="Remove the given types of entities from results",
        examples=[["Vessel"]],
    )


class EntityMatchQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: QueryEntity
    algorithm: str = Field(settings.DEFAULT_ALGORITHM, title=ALGO_HELP)
    threshold: float = Field(
        settings.SCORE_THRESHOLD,
        ge=0.0,
        le=1.0,
        title="Score threshold for results to be considered matches",
    )
    cutoff: float = Field(
        settings.SCORE_THRESHOLD,
        ge=0.0,
        le=1.0,
        title="Lower bound of score for results to be returned at all",
        description="Must not be above the threshold.",
    )
    limit: int = Field(
        settings.MATCH_PAGE,
        le=settings.MAX_MATCHES,
        title="Number of results to return",
    )
    matcher: MatcherOptions = Field(default=MatcherOptions())
    filters: Filters = Field(default=Filters())

    @model_validator(mode="after")
    def cutoff_not_above_threshold(self) -> "EntityMatchQuery":
        if self.cutoff > self.threshold:
            raise ValueError("cutoff must not be above threshold")
        return self


class EntityMatches(BaseModel):
    results: list[ScoredEntityResponse]
    total: TotalSpec
    query: QueryEntity
