from typing import Type
from fastapi import Path, Query
from fastapi import HTTPException
from nomenklatura.matching import ALGORITHMS, ScoringAlgorithm, get_algorithm

from yente import settings
from yente.data import get_catalog
from yente.data.dataset import Dataset


PATH_DATASET = Path(
    description="Data source or collection name to scope the query to.",
    examples=["default"],
)
QUERY_PREFIX = Query("", min_length=0, description="Search prefix")
TS_PATTERN = r"^\d{4}-\d{2}-\d{2}(T\d{2}(:\d{2}(:\d{2})?)?)?$"
ALGO_LIST = ", ".join([a.NAME for a in ALGORITHMS])
ALGO_HELP = (
    f"Scoring algorithm to use, options: {ALGO_LIST} (best: {settings.BEST_ALGORITHM})"
)


def get_algorithm_by_name(name: str) -> Type[ScoringAlgorithm]:
    """Return the scoring algorithm class with the given name."""
    name = name.lower().strip()
    if name == "best":
        name = settings.BEST_ALGORITHM
    algorithm = get_algorithm(name)
    if algorithm is None:
        raise HTTPException(400, detail=f"Invalid algorithm: {name}")
    return algorithm


async def get_dataset(name: str) -> Dataset:
    catalog = await get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset
