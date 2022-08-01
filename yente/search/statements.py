from typing import Any, Dict, List, Union, Optional

from yente import settings
from yente.data.dataset import Dataset
from yente.data.statements import StatementResponse, StatementModel
from yente.search.base import get_es, get_opaque_id
from yente.search.search import result_total


def statement_query(
    dataset: Optional[Dataset] = None, **kwargs: Union[None, str, bool]
) -> Dict[str, Any]:
    filters: List[Dict[str, Any]] = []
    if dataset is not None:
        filters.append({"terms": {"dataset": dataset.dataset_names}})
    for field, value in kwargs.items():
        if value is not None:
            filters.append({"term": {field: value}})
    if not len(filters):
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


async def statement_results(
    query: Dict[str, Any], limit: int, offset: int, sort: List[Any]
) -> StatementResponse:
    es = await get_es()
    es_ = es.options(opaque_id=get_opaque_id())
    resp = await es_.search(
        index=settings.STATEMENT_INDEX,
        query=query,
        size=limit,
        from_=offset,
        sort=sort,
    )
    return StatementResponse(
        results=StatementModel.from_search(resp),
        total=result_total(resp),
        limit=limit,
        offset=offset,
    )
