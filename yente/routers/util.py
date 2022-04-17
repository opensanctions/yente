import structlog
from fastapi import Path, Query
from fastapi import HTTPException

from yente import settings
from yente.data.dataset import Dataset
from yente.data import get_datasets


log: structlog.stdlib.BoundLogger = structlog.get_logger("yente")

PATH_DATASET = Path(
    settings.SCOPE_DATASET,
    description="Data source or collection name",
    example=settings.SCOPE_DATASET,
)
QUERY_PREFIX = Query("", min_length=1, description="Search prefix")


async def get_dataset(name: str) -> Dataset:
    datasets = await get_datasets()
    dataset = datasets.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset
