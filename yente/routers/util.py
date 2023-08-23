from fastapi import Path, Query
from fastapi import HTTPException

from yente.data.dataset import Dataset
from yente.data import get_catalog


PATH_DATASET = Path(
    description="Data source or collection name to be queries",
    examples=["default"],
)
QUERY_PREFIX = Query("", min_length=1, description="Search prefix")
TS_PATTERN = r"^\d{4}-\d{2}-\d{2}(T\d{2}(:\d{2}(:\d{2})?)?)?$"


async def get_dataset(name: str) -> Dataset:
    catalog = await get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset
