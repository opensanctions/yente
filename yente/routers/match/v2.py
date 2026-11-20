from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from followthemoney import model
from followthemoney.dataset import (
    DatasetQuery,
    evaluate_query,
    parse_query,
    validate_query,
)
from followthemoney.exc import InvalidDatasetQuery
from nomenklatura.matching.types import ScoringConfig

from yente import settings
from yente.data import get_catalog
from yente.data.common import EntityExample, ErrorResponse, TotalSpec
from yente.data.dataset import Dataset
from yente.data.match_v2 import (
    EntityMatches,
    EntityMatchQuery,
    QueryEntity,
    RequestWarning,
)
from yente.logs import get_logger
from yente.routers.match import (
    build_match_query,
    candidate_count,
    get_algorithm,
    score_candidates,
)
from yente.routers.util import ProviderDep
from yente.search.queries import FilterSpec

log = get_logger(__name__)
router = APIRouter(prefix="/v2")


def check_query_params(request: Request, strict: bool) -> list[RequestWarning]:
    """All options go in the request body, so query string parameters are an
    error, or ignored with a warning when strict mode is off."""
    problems = [
        RequestWarning(
            type="extra_forbidden",
            loc=["query", name],
            msg="Extra inputs are not permitted; options go in the request body",
            input=value,
        )
        for name, value in request.query_params.multi_items()
    ]
    return report(problems, strict, "ignored")


def report(
    problems: list[RequestWarning], strict: bool, fix: str
) -> list[RequestWarning]:
    """Raise the problems as validation errors in strict mode, otherwise return
    them as warnings, with what was done about them."""
    if strict and problems:
        raise RequestValidationError([p.model_dump() for p in problems])
    return [p.model_copy(update={"msg": f"{p.msg}, {fix}"}) for p in problems]


def check_query_entity(
    entity: QueryEntity, strict: bool
) -> tuple[QueryEntity, list[RequestWarning]]:
    """Check the query entity against the FollowTheMoney model. Unknown
    properties and values the property's type can't parse are errors, or are
    discarded with a warning when strict mode is off."""
    loc: list[str | int] = ["body", "query"]
    schema = model.get(entity.schema_)
    if schema is None or not (schema.matchable or schema.name == settings.BASE_SCHEMA):
        reason = "Unknown" if schema is None else "Non-matchable"
        error = RequestWarning(
            type="invalid_schema",
            loc=[*loc, "schema"],
            msg=f"{reason} schema: {entity.schema_!r}",
            input=entity.schema_,
        )
        raise RequestValidationError([error.model_dump()])

    problems: list[RequestWarning] = []
    properties: dict[str, list[str]] = {}
    for name, values in entity.properties.items():
        prop = schema.get(name)
        if prop is None:
            problems.append(
                RequestWarning(
                    type="unknown_property",
                    loc=[*loc, "properties", name],
                    msg=f"Unknown property for {schema.name}: {name!r}",
                    input=values,
                )
            )
            continue
        for index, value in enumerate(values):
            # Clean the same way the query entity is built, so that values
            # like "Russia" for a country are accepted.
            if prop.type.clean_text(value, fuzzy=True, format=prop.format) is None:
                problems.append(
                    RequestWarning(
                        type="invalid_value",
                        loc=[*loc, "properties", name, index],
                        msg=f"Invalid {prop.type.label.lower()} value: {value!r}",
                        input=value,
                    )
                )
            else:
                properties.setdefault(name, []).append(value)
    warnings = report(problems, strict, "ignored")
    if not properties:
        error = RequestWarning(
            type="no_valid_properties",
            loc=[*loc, "properties"],
            msg="None of the properties are valid",
            input=entity.properties,
        )
        raise RequestValidationError([error.model_dump()])
    checked = QueryEntity(schema=entity.schema_, properties=properties)
    return checked, warnings


async def resolve_scope(datasets: str | DatasetQuery) -> set[Dataset]:
    """The leaf datasets selected by the request's dataset query.

    A query that doesn't parse is a malformed request (400). A well-formed query
    naming a dataset that isn't in the catalog is an invalid value (422).
    """
    try:
        query = parse_query(datasets) if isinstance(datasets, str) else datasets
        validate_query(query)
    except InvalidDatasetQuery as exc:
        raise HTTPException(400, detail=f"Invalid dataset query: {exc}")
    catalog = await get_catalog()
    try:
        return evaluate_query(catalog, query)
    except InvalidDatasetQuery as exc:
        # The query is valid, so evaluating it only fails on an unknown dataset.
        error = {
            "type": "unknown_dataset",
            "loc": ("body", "filters", "datasets"),
            "msg": str(exc),
            "input": datasets,
        }
        raise RequestValidationError([error])


@router.post(
    "/match",
    summary="Query by example matcher",
    tags=["Matching"],
    response_model=EntityMatches,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def match(
    request: Request,
    provider: ProviderDep,
    match: EntityMatchQuery,
) -> EntityMatches:
    """Match entities based on a complex set of criteria, like name, date of birth
    and nationality of a person. This works by submitting an entity formatted like
    those returned by the API.

    Tutorials:
    * [Using the matching API](https://www.opensanctions.org/docs/api/matching/)
    * [Configuring the scoring system](https://www.opensanctions.org/docs/api/scoring/)

    Each request matches a single entity. For example, the following would be a
    valid request:

    ```json
    {
        "query": {
            "schema": "Person",
            "properties": {
                "name": ["John Doe"],
                "birthDate": ["1975-04-21"],
                "nationality": ["us"]
            }
        },
        "filters": {
            "datasets": ["#list.sanction"]
        }
    }
    ```

    The datasets to match against are selected using a
    [dataset query](https://followthemoney.tech/docs/metadata/#dataset-query-dsl).
    This is either a string such as `"default-#list.pep"`, or its structured
    form: a list of dataset names, collection names and `#tag` selectors, or a
    combination of them using `or`, `and` and `not`, e.g.
    `{"and": ["default", {"not": "#list.pep"}]}`. If no datasets are given, the
    `default` collection is used.

    The query entity is checked against the FollowTheMoney model: the schema must
    be matchable, every property must exist in the schema, and every value must
    be valid for the property's type, e.g. `1975-04-21` for a date. An invalid
    request gets a 422 response listing each problem with its location in the
    request.

    With `"strict_mode": false`, problems that can be ignored are ignored
    instead: unexpected fields and query string parameters, invalid properties
    and values, and invalid `threshold` and `cutoff` values. Each is listed in
    the response `warnings`, in the same form as a validation error. A missing
    query, schema or properties, or an unknown schema, is always an error.

    The response contains the scored results and the query entity as it was
    understood by the matcher:

    ```json
    {
        "query": {},
        "results": [...],
        "total": {"value": 1, "relation": "eq"}
    }
    ```

    The precision of the results will be dependent on the amount of detail submitted
    with each example. The following properties are most helpful for particular types:

    * **Person**: ``name``, ``birthDate``, ``nationality``, ``idNumber``, ``address``
    * **Organization**: ``name``, ``country``, ``registrationNumber``, ``address``
    * **Company**: ``name``, ``jurisdiction``, ``registrationNumber``, ``address``,
      ``incorporationDate``
    """
    filters = match.filters
    warnings = [*match.warnings, *check_query_params(request, match.strict_mode)]
    query_entity, entity_warnings = check_query_entity(match.query, match.strict_mode)
    warnings.extend(entity_warnings)
    scope = await resolve_scope(filters.datasets)
    algorithm = get_algorithm(match.algorithm, match.matcher.config)
    topics: list[FilterSpec] = [("topics", t) for t in filters.topics]

    entity, query = build_match_query(
        example=EntityExample(
            id=None,
            schema=query_entity.schema_,
            properties=dict[str, str | list[Any]](query_entity.properties),
        ),
        dataset_names={ds.name for ds in scope},
        filters=topics,
        exclude_schema=filters.exclude_schemata,
        changed_since=filters.changed_since,
        exclude_entity_ids=filters.exclude_entities,
    )
    total, scored = await score_candidates(
        provider,
        entity,
        query,
        algorithm,
        ScoringConfig(weights=match.matcher.weights, config=match.matcher.config),
        candidates=candidate_count(match.limit),
        limit=match.limit,
        threshold=match.threshold,
        cutoff=match.cutoff,
        metric_attributes={"endpoint": "match_v2"},
    )
    log.info(
        "/v2/match",
        action="match",
        schema=entity.schema.name,
        results=total,
        threshold=match.threshold,
        datasets=filters.datasets,
        limit=match.limit,
        # Log the algorithm passed in the request, which may be "best"
        algorithm_param=match.algorithm,
    )
    return EntityMatches(
        results=scored,
        total=TotalSpec(value=total, relation="eq"),
        query=QueryEntity(
            schema=entity.schema.name, properties=dict(entity.properties)
        ),
        warnings=warnings,
    )
