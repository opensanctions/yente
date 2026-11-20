from copy import deepcopy
from typing import Annotated, Any, Self

from followthemoney.util import sanitize_text
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    StringConstraints,
    ValidationError,
    model_validator,
)
from pydantic_core import ErrorDetails

from yente import settings
from yente.data.common import ScoredEntityResponse, TotalSpec
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

    # A string is a query in the string syntax, parsed by
    # followthemoney.dataset.parse_query. A list or object is the structured
    # form of followthemoney.dataset.DatasetQuery.
    datasets: str | list[Any] | dict[str, Any] = Field(
        default=["default"],
        description=(
            "The datasets to match against, as a FollowTheMoney dataset query. "
            "Either a string such as `default-#list.pep`, or the structured form: "
            "a list of dataset names, collection names and `#tag` selectors, "
            "or an `or`/`and`/`not` operator object. "
            "Defaults to the `default` collection. "
            "See https://followthemoney.tech/docs/metadata/#dataset-query-dsl"
        ),
        examples=[
            "default-#list.pep",
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
    """A problem with the request that was ignored because strict mode is off.
    Shaped like a validation error, with the location in the request."""

    type: str = Field(examples=["extra_forbidden"])
    loc: list[str | int] = Field(examples=[["body", "query", "id"]])
    msg: str = Field(examples=["Extra inputs are not permitted, ignored"])
    input: Any = None


def _lenient_fix(error: ErrorDetails) -> str | None:
    """How a validation error is handled when strict mode is off: the suffix
    for the warning message, or None if the error can't be ignored."""
    loc = error["loc"]
    if error["type"] == "extra_forbidden":
        return "ignored"
    if loc in (("threshold",), ("cutoff",)) and error["type"] in (
        "less_than_equal",
        "greater_than_equal",
    ):
        return "using the default"
    # A property's values, or a single value.
    if loc[:2] == ("query", "properties") and len(loc) > 2:
        return "ignored"
    return None


def _discard(data: Any, loc: tuple[str | int, ...]) -> None:
    """Remove the value at loc from the request data."""
    parent = data
    for key in loc[:-1]:
        try:
            parent = parent[key]
        except (KeyError, IndexError, TypeError):
            return
    if isinstance(parent, list) and isinstance(loc[-1], int):
        del parent[loc[-1]]
        # Remove a list left empty too, rather than report it separately.
        if not parent:
            _discard(data, loc[:-1])
    elif isinstance(parent, dict):
        parent.pop(loc[-1], None)
    elif len(loc) > 1:
        # A single value given in place of a list, reported at index 0.
        _discard(data, loc[:-1])


class EntityMatchQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: QueryEntity
    strict_mode: bool = Field(
        default=True,
        title="Reject requests with invalid or unexpected values",
        description=(
            "When false, invalid or unexpected values that can be ignored are "
            "discarded instead, and each is reported in the response `warnings`. "
            "A missing query, schema or properties is always an error."
        ),
    )
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

    _warnings: list[RequestWarning] = PrivateAttr(default_factory=list)

    @property
    def warnings(self) -> list[RequestWarning]:
        """The problems ignored because strict mode is off."""
        return self._warnings

    @model_validator(mode="wrap")
    @classmethod
    def lenient(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        """When strict mode is off, discard the values that fail validation in a
        way that can be ignored, and validate again."""
        warnings: list[RequestWarning] = []
        # Discarding an invalid value can leave an invalid parent, like an
        # empty list of values, so this may take more than one round.
        for _ in range(3):
            try:
                result = handler(data)
                result._warnings = [*warnings, *result._warnings]
                return result
            except ValidationError as exc:
                if not isinstance(data, dict) or data.get("strict_mode", True):
                    raise
                ignorable = [(e, _lenient_fix(e)) for e in exc.errors()]
                ignorable = [(e, fix) for e, fix in ignorable if fix is not None]
                if not ignorable:
                    raise
                data = deepcopy(data)
                # Deepest and last first, so the remaining locations stay valid.
                for error, fix in sorted(ignorable, key=lambda p: p[0]["loc"])[::-1]:
                    _discard(data, error["loc"])
                    warnings.append(
                        RequestWarning(
                            type=error["type"],
                            loc=["body", *error["loc"]],
                            msg=f"{error['msg']}, {fix}",
                            input=error["input"],
                        )
                    )
        return handler(data)

    @model_validator(mode="after")
    def cutoff_not_above_threshold(self) -> Self:
        if self.cutoff > self.threshold:
            if self.strict_mode:
                raise ValueError("cutoff must not be above threshold")
            self._warnings.append(
                RequestWarning(
                    type="value_error",
                    loc=["body", "cutoff"],
                    msg="cutoff must not be above threshold, using the threshold",
                    input=self.cutoff,
                )
            )
            self.cutoff = self.threshold
        return self


class EntityMatches(BaseModel):
    results: list[ScoredEntityResponse]
    total: TotalSpec
    query: QueryEntity
    warnings: list[RequestWarning] = Field(
        default=[],
        description="Problems with the request that were ignored because "
        "`strict_mode` is false.",
    )
