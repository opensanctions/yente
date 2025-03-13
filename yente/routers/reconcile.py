import json
import asyncio
from urllib.parse import urljoin
from typing import Any, Coroutine, Dict, List, Tuple, Optional, Union
from fastapi import APIRouter, Query, Form, Depends
from fastapi import Request, Response
from fastapi import HTTPException
from followthemoney import model
from followthemoney.types import registry


from yente import settings
from yente.data.common import ErrorResponse, EntityExample
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.data.freebase import (
    FreebaseEntity,
    FreebaseEntityResult,
    FreebaseExtendPropertiesResponse,
    FreebaseExtendProperty,
    FreebaseExtendQuery,
    FreebaseExtendResponse,
    FreebaseExtendResponseMeta,
    FreebaseExtendResponseValue,
    FreebaseManifestExtend,
    FreebaseManifestExtendPropertySetting,
    FreebaseManifestExtendPropertySettingChoice,
    FreebaseManifestExtendProposeProperties,
    FreebaseManifestView,
    FreebaseManifestPreview,
    FreebaseManifestSuggest,
    FreebaseManifestSuggestType,
    FreebaseProperty,
    FreebaseRenderMethod,
    FreebaseScoredEntity,
    FreebaseType,
    FreebaseEntitySuggestResponse,
    FreebasePropertySuggestResponse,
    FreebaseTypeSuggestResponse,
    FreebaseManifest,
)
from yente.search.queries import entity_query, prefix_query
from yente.search.search import (
    get_entity,
    search_entities,
    result_entities,
    result_total,
)
from yente.search.search import get_matchable_schemata
from yente.provider import SearchProvider, get_provider
from yente.scoring import score_results
from yente.util import EntityRedirect, match_prefix, limit_window, typed_url
from yente.routers.util import PATH_DATASET, QUERY_PREFIX
from yente.routers.util import TS_PATTERN, ALGO_HELP
from yente.routers.util import get_algorithm_by_name, get_dataset


log = get_logger(__name__)
router = APIRouter()


@router.get(
    "/reconcile/{dataset}",
    summary="Reconciliation manifest",
    tags=["Reconciliation"],
    response_model=FreebaseManifest,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def reconcile(
    request: Request,
    dataset: str = PATH_DATASET,
    provider: SearchProvider = Depends(get_provider),
) -> FreebaseManifest:
    """Reconciliation API, emulates Google Refine API. This endpoint can be used
    to bulk match entities against the system using an end-user application like
    [OpenRefine](https://openrefine.org). The reconciliation API uses the same
    search and matching functions as the matching API and will also produce
    scores that reflect additional properties like country or date of birth, if
    specified.

    Tutorial: [Using OpenRefine to match entities in a spreadsheet](https://www.opensanctions.org/articles/2022-01-10-openrefine-reconciliation/).
    """
    ds = await get_dataset(dataset)
    base_url = typed_url(urljoin(str(request.base_url), f"/reconcile/{dataset}"))
    schemata = await get_matchable_schemata(provider, ds)
    # Pass on query string (useful for API keys)
    query_string = request.url.query.strip()
    if len(query_string):
        query_string = f"?{query_string}"

    return FreebaseManifest(
        versions=["0.2"],
        name=f"{ds.title} ({settings.TITLE})",
        identifierSpace=typed_url("https://www.opensanctions.org/reference/#schema"),
        schemaSpace=typed_url("https://www.opensanctions.org/reference/#schema"),
        view=FreebaseManifestView(url="https://www.opensanctions.org/entities/{{id}}/"),
        documentation=typed_url("https://www.opensanctions.org/docs/"),
        batchSize=settings.DEFAULT_PAGE,
        preview=FreebaseManifestPreview(
            url="https://www.opensanctions.org/entities/preview/{{id}}/",
            width=430,
            height=600,
        ),
        suggest=FreebaseManifestSuggest(
            entity=FreebaseManifestSuggestType(
                service_url=base_url, service_path=f"/suggest/entity{query_string}"
            ),
            type=FreebaseManifestSuggestType(
                service_url=base_url, service_path=f"/suggest/type{query_string}"
            ),
            property=FreebaseManifestSuggestType(
                service_url=base_url, service_path=f"/suggest/property{query_string}"
            ),
        ),
        extend=FreebaseManifestExtend(
            propose_properties=FreebaseManifestExtendProposeProperties(
                service_url=base_url, service_path=f"/extend/property{query_string}"
            ),
            propose_settings=[
                FreebaseManifestExtendPropertySetting(
                    name="limit",
                    label="Limit",
                    type="number",
                    default=0,
                    help_text="Maximum number of values to return per row (0 for no limit).",
                ),
                FreebaseManifestExtendPropertySetting(
                    name="render",
                    label="Value rendering",
                    type="select",
                    default=FreebaseRenderMethod.caption,
                    choices=[
                        FreebaseManifestExtendPropertySettingChoice(
                            id=FreebaseRenderMethod.caption, name="User-readable value"
                        ),
                        FreebaseManifestExtendPropertySettingChoice(
                            id=FreebaseRenderMethod.raw, name="Machine-readable value"
                        ),
                    ],
                    help_text="Return readable value (e.g. 'Russia') instead of raw value ('ru').",
                ),
            ],
        ),
        defaultTypes=[FreebaseType.from_schema(s) for s in schemata],
    )


@router.post(
    "/reconcile/{dataset}",
    summary="Reconciliation queries",
    tags=["Reconciliation"],
    response_model=Union[Dict[str, FreebaseEntityResult], FreebaseExtendResponse],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
    include_in_schema=False,
)
async def reconcile_post(
    response: Response,
    dataset: str = PATH_DATASET,
    queries: str = Form(None, description="JSON-encoded reconciliation queries"),
    extend: str = Form(None, description="JSON-encoded reconciliation queries"),
    algorithm: str = Query(
        settings.BEST_ALGORITHM,
        title=ALGO_HELP,
    ),
    changed_since: Optional[str] = Query(
        None,
        pattern=TS_PATTERN,
        title="Match against entities that were updated since the given date",
    ),
    provider: SearchProvider = Depends(get_provider),
) -> Union[Dict[str, FreebaseEntityResult], FreebaseExtendResponse]:
    """Reconciliation API, emulates Google Refine API. This endpoint is used by
    clients for matching, refer to the discovery endpoint for details."""
    if extend is not None and len(extend.strip()):
        extend_resp = await reconcile_extend(provider, extend)
        response.headers["x-batch-size"] = str(len(extend_resp.rows))
        return extend_resp

    ds = await get_dataset(dataset)
    resp = await reconcile_queries(provider, ds, queries, algorithm, changed_since)
    response.headers["x-batch-size"] = str(len(resp))
    return resp


async def reconcile_queries(
    provider: SearchProvider,
    dataset: Dataset,
    data: str,
    algorithm: str,
    changed_since: Optional[str],
) -> Dict[str, FreebaseEntityResult]:
    # multiple requests in one query
    try:
        queries: Dict[str, Dict[str, Any]] = json.loads(data)
    except (TypeError, ValueError):
        raise HTTPException(400, detail="Cannot decode query")

    if len(queries) > settings.MAX_BATCH:
        msg = "Too many queries in one batch (limit: %d)" % settings.MAX_BATCH
        raise HTTPException(400, detail=msg)

    tasks: List[Coroutine[Any, Any, Tuple[str, FreebaseEntityResult]]] = []
    for k, q in queries.items():
        task = reconcile_query(provider, k, dataset, q, algorithm, changed_since)
        tasks.append(task)
    results: List[Tuple[str, FreebaseEntityResult]] = await asyncio.gather(*tasks)
    return dict(results)


async def reconcile_query(
    provider: SearchProvider,
    name: str,
    dataset: Dataset,
    query: Dict[str, Any],
    algorithm: str,
    changed_since: Optional[str],
) -> Tuple[str, FreebaseEntityResult]:
    """Reconcile operation for a single query."""
    limit, offset = limit_window(query.get("limit"), 0, settings.MAX_MATCHES)
    schema = query.get("type", settings.BASE_SCHEMA)
    properties: Dict[str, List[str]] = {"alias": [query.get("query", "")]}

    for p in query.get("properties", []):
        prop = model.get_qname(p.get("pid"))
        if prop is None:
            continue
        if prop.name not in properties:
            properties[prop.name] = []
        properties[prop.name].append(p.get("v"))

    example = EntityExample(id=None, schema=schema, properties=dict(properties))
    try:
        proxy = Entity.from_example(example)
        query = entity_query(dataset, proxy, changed_since=changed_since)
    except Exception as exc:
        raise HTTPException(400, detail=str(exc))
    resp = await search_entities(provider, query, limit=limit, offset=offset)
    algorithm_ = get_algorithm_by_name(algorithm)
    entities = result_entities(resp)
    total, scoreds = score_results(algorithm_, proxy, entities, limit=limit)
    results = [FreebaseScoredEntity.from_scored(s) for s in scoreds]
    log.info(
        f"/reconcile/{dataset.name}",
        action="reconcile",
        schema=proxy.schema.name,
        matches=total,
    )
    return name, FreebaseEntityResult(result=results)


async def reconcile_extend(
    provider: SearchProvider,
    data: str,
) -> FreebaseExtendResponse:
    try:
        extendq: Any = json.loads(data)
    except (TypeError, ValueError):
        raise HTTPException(400, detail="Cannot decode extension request")
    query = FreebaseExtendQuery.model_validate(extendq)

    if len(query.ids) > settings.MAX_BATCH:
        msg = "Too many queries in one batch (limit: %d)" % settings.MAX_BATCH
        raise HTTPException(400, detail=msg)

    try:
        queries = [get_entity(provider, entity_id) for entity_id in query.ids]
        entities = await asyncio.gather(*queries)
    except EntityRedirect:
        msg = "Please specify the canonical entity ID, not a referent"
        raise HTTPException(400, detail=msg)

    metas: Dict[str, FreebaseExtendResponseMeta] = {}
    resp = FreebaseExtendResponse(meta=[], rows={e: {} for e in query.ids})
    for entity in entities:
        if entity is None:
            continue
        row: Dict[str, List[FreebaseExtendResponseValue]] = {}
        for qprop in query.properties:
            prop = entity.schema.get(qprop.id)
            if prop is None:
                continue
            if qprop.id not in metas:
                metas[qprop.id] = FreebaseExtendResponseMeta(
                    id=prop.name, name=prop.label
                )
            values = entity.get(prop.name)
            if qprop.settings.limit > 0:
                values = values[: qprop.settings.limit]
            if qprop.settings.render == FreebaseRenderMethod.caption:
                values = [prop.type.caption(v) or v for v in values]
            row[qprop.id] = [FreebaseExtendResponseValue(str=v) for v in values]
        resp.rows[entity.id] = row
    resp.meta = list(metas.values())
    return resp


@router.get(
    "/reconcile/{dataset}/suggest/entity",
    summary="Suggest entity",
    tags=["Reconciliation"],
    response_model=FreebaseEntitySuggestResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
    include_in_schema=False,
)
async def reconcile_suggest_entity(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    limit: int = Query(
        settings.MATCH_PAGE,
        description="Number of suggestions to return",
        le=settings.MAX_PAGE,
    ),
    provider: SearchProvider = Depends(get_provider),
) -> FreebaseEntitySuggestResponse:
    """Suggest an entity based on a text query. This is functionally very
    similar to the basic search API, but returns data in the structure assumed
    by the community specification.

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    ds = await get_dataset(dataset)
    results = []
    query = prefix_query(ds, prefix)
    limit, offset = limit_window(limit, 0, settings.MATCH_PAGE)
    resp = await search_entities(provider, query, limit=limit, offset=offset)
    for result in result_entities(resp):
        results.append(FreebaseEntity.from_proxy(result))
    log.info(
        f"/reconcile/{ds.name}/suggest/entity",
        action="suggest",
        length=len(prefix),
        dataset=ds.name,
        results=result_total(resp).value,
    )
    return FreebaseEntitySuggestResponse(prefix=prefix, result=results)


@router.get(
    "/reconcile/{dataset}/suggest/property",
    summary="Suggest property",
    tags=["Reconciliation"],
    response_model=FreebasePropertySuggestResponse,
    include_in_schema=False,
)
async def reconcile_suggest_property(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    provider: SearchProvider = Depends(get_provider),
) -> FreebasePropertySuggestResponse:
    """Given a search prefix, return all the type/schema properties which match
    the given text. This is used to auto-complete property selection for detail
    filters in OpenRefine."""
    ds = await get_dataset(dataset)
    schemata = await get_matchable_schemata(provider, ds)
    matches: List[FreebaseProperty] = []
    for prop in model.properties:
        if prop.schema not in schemata:
            continue
        if prop.hidden or prop.type == prop.type == registry.entity:
            continue
        if match_prefix(prefix, prop.name, prop.label):
            matches.append(FreebaseProperty.from_prop(prop))
    result = matches[: settings.MATCH_PAGE]
    return FreebasePropertySuggestResponse(prefix=prefix, result=result)


@router.get(
    "/reconcile/{dataset}/suggest/type",
    summary="Suggest type (schema)",
    tags=["Reconciliation"],
    response_model=FreebaseTypeSuggestResponse,
    include_in_schema=False,
)
async def reconcile_suggest_type(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    provider: SearchProvider = Depends(get_provider),
) -> FreebaseTypeSuggestResponse:
    """Given a search prefix, return all the types (i.e. schema) which match
    the given text. This is used to auto-complete type selection for the
    configuration of reconciliation in OpenRefine."""
    ds = await get_dataset(dataset)
    matches: List[FreebaseType] = []
    for schema in await get_matchable_schemata(provider, ds):
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(FreebaseType.from_schema(schema))
    result = matches[: settings.MATCH_PAGE]
    return FreebaseTypeSuggestResponse(prefix=prefix, result=result)


@router.get(
    "/reconcile/{dataset}/extend/property",
    summary="Extend properties proposal",
    tags=["Reconciliation"],
    response_model=FreebaseExtendPropertiesResponse,
    include_in_schema=False,
)
async def reconcile_extend_properties(
    dataset: str = PATH_DATASET,
    type: str = Query(
        settings.BASE_SCHEMA,
        min_length=0,
        description="Type of the entity for which properties should be proposed.",
    ),
    limit: int = Query(
        0,
        description="Number of suggestions to return.",
    ),
) -> FreebaseExtendPropertiesResponse:
    """Given a type (schema), suggest a set of properties that could be retrieved
    for data extension."""
    schema = model.get(type)
    if schema is None:
        raise HTTPException(400, detail="Invalid type: %s" % type)
    properties: List[FreebaseExtendProperty] = []
    for featured in schema.featured:
        prop = schema.get(featured)
        if prop is not None:
            properties.append(FreebaseExtendProperty(id=prop.name, name=prop.label))
    for prop in schema.properties.values():
        if prop.hidden or prop.type == registry.entity:
            continue
        if prop.name not in schema.featured:
            properties.append(FreebaseExtendProperty(id=prop.name, name=prop.label))
    if limit > 0:
        properties = properties[:limit]
    properties = sorted(properties, key=lambda p: p.name)
    return FreebaseExtendPropertiesResponse(
        limit=limit, type=schema.name, properties=properties
    )
