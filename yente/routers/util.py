from fastapi import Path, Query
from fastapi import HTTPException

from yente.data.dataset import Dataset
from yente.data import get_catalog


PATH_DATASET = Path(
    "default",
    description="Data source or collection name to be queries",
    example="default",
)
QUERY_PREFIX = Query("", min_length=1, description="Search prefix")


async def get_dataset(name: str) -> Dataset:
    catalog = await get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset
