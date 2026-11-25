from typing import Annotated, Any, Literal

from followthemoney.util import sanitize_text
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
)

from yente import settings
from yente.data.common import (
    ScoredEntity,
    ScoringConfigValues,
    ScoringWeights,
    TotalSpec,
)
from yente.routers.util import ALGO_HELP, TS_PATTERN


def _number_as_str(value: Any) -> Any:
    # Format numbers the way FtM does when the query entity is built. bool is a
    # subclass of int, but True isn't a meaningful property value.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return sanitize_text(value)
    return value


def _as_list(value: Any) -> Any:
    """A single value is shorthand for a list containing it."""
    return value if isinstance(value, list) else [value]


PropertyValue = Annotated[
    str,
    BeforeValidator(_number_as_str, json_schema_input_type=str | int | float),
    StringConstraints(strip_whitespace=True, min_length=1),
]
PropertyValues = Annotated[
    list[PropertyValue],
    Field(min_length=1),
    BeforeValidator(
        _as_list, json_schema_input_type=PropertyValue | list[PropertyValue]
    ),
]


class QueryEntity(BaseModel):
    """An entity to match against, formatted like the entities the API returns.

    The schema, property names and values are checked against the
    FollowTheMoney model when the request is handled.
    """

    model_config = ConfigDict(extra="forbid")

    schema_: str = Field(min_length=1, alias="schema", examples=["Person"])
    properties: dict[str, PropertyValues] = Field(
        min_length=1, examples=[{"name": ["John Doe"], "birthDate": ["1975-04-21"]}]
    )


class MatcherOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weights: ScoringWeights = Field(
        default={},
        description="Override the weights of features in the scoring algorithm.",
        examples=[{"name_literal": 0.8}],
    )
    config: ScoringConfigValues = Field(
        default={},
        description="Algorithm-specific configuration parameters.",
        examples=[{"nm_number_mismatch": 0.4}],
    )


class Filters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # A string is a query in the string syntax, parsed by
    # followthemoney.dataset.parse_query. A list or object is the structured
    # form of followthemoney.dataset.DatasetQuery.
    datasets: str | list[Any] | dict[str, Any] | None = Field(
        default=None,
        description=(
            "The datasets to match against, as a FollowTheMoney dataset query. "
            "Either a string such as `default -#list.pep`, or the structured form: "
            "a list of dataset names, collection names and `#tag` selectors, "
            "or an `or`/`and`/`not` operator object. "
            "Defaults to the `default` collection, unless the yente instance is "
            "configured otherwise. "
            "See https://followthemoney.tech/docs/metadata/#dataset-query-dsl"
        ),
        examples=[
            "default -#list.pep",
            ["#list.sanction"],
            {"and": ["default", {"not": "#list.pep"}]},
        ],
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


class RequestWarning(BaseModel):
    """A problem with the request that was handled rather than rejected, shaped
    like a validation error, with its location in the request."""

    type: str = Field(examples=["invalid_value"])
    loc: list[str | int] = Field(
        examples=[["body", "query", "properties", "birthDate", 0]]
    )
    msg: str = Field(examples=["Invalid date value: 'yesterday'"])
    input: Any = None


class EntityMatchQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: QueryEntity
    invalid_values: Literal["reject", "drop"] = Field(
        default="reject",
        title="How to handle invalid query entity property values",
        description=(
            "With `reject`, values that aren't valid for their property's type "
            "are rejected with a 422 response. With `drop`, they're dropped, "
            "and each is listed in the response `warnings`. If no valid values "
            "are left, the request is rejected as with `reject`. Unknown "
            "properties are rejected either way."
        ),
    )
    algorithm: str = Field(settings.DEFAULT_ALGORITHM, title=ALGO_HELP)
    threshold: float = Field(
        settings.SCORE_THRESHOLD,
        ge=0.0,
        le=1.0,
        title="Score threshold for results to be considered matches",
    )
    limit: int = Field(
        settings.MATCH_PAGE,
        le=settings.MAX_MATCHES,
        title="Number of results to return",
    )
    matcher: MatcherOptions = Field(default=MatcherOptions())
    filters: Filters = Field(default=Filters())


class EntityMatches(BaseModel):
    # Only matches are returned, so results don't say whether they're a match.
    # Pydantic serializes the ScoredEntityResponse results by this declared
    # type, leaving out `match`.
    results: list[ScoredEntity]
    total: TotalSpec
    query: QueryEntity
    warnings: list[RequestWarning] = Field(
        default=[],
        description=(
            "The query entity property values that were dropped because "
            "`invalid_values` is `drop`."
        ),
    )
