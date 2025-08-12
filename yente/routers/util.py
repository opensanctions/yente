from typing import List, Type
from fastapi import Path, Query
from fastapi import HTTPException
from nomenklatura.matching.logic_v2.model import LogicV2
from nomenklatura.matching import (
    ScoringAlgorithm,
    LogicV1,
    NameMatcher,
    NameQualifiedMatcher,
    RegressionV1,
)

from yente import settings
from yente.data import get_catalog
from yente.data.dataset import Dataset

ENABLED_ALGORITHMS: List[Type[ScoringAlgorithm]] = [
    LogicV1,
    LogicV2,
    NameMatcher,
    NameQualifiedMatcher,
    RegressionV1,
]

PATH_DATASET = Path(
    description="Data source or collection name to scope the query to.",
    examples=["default"],
)
QUERY_PREFIX = Query("", min_length=0, description="Search prefix")
TS_PATTERN = r"^\d{4}-\d{2}-\d{2}(T\d{2}(:\d{2}(:\d{2})?)?)?$"
ALGO_LIST = ", ".join([a.NAME for a in ENABLED_ALGORITHMS])
ALGO_HELP = (
    f"Scoring algorithm to use, options: {ALGO_LIST} (best: {settings.BEST_ALGORITHM})"
)

# Ensure that all hidden algorithms are valid
assert all(
    algo in [a.NAME for a in ENABLED_ALGORITHMS] for algo in settings.HIDDEN_ALGORITHMS
), "Invalid algorithm name in YENTE_HIDDEN_ALGORITHMS"


def get_algorithm_by_name(name: str) -> Type[ScoringAlgorithm]:
    """Return the scoring algorithm class with the given name."""
    name_clean = name.lower().strip()
    if name_clean == "best":
        name_clean = settings.BEST_ALGORITHM
    algorithm = None
    for a in ENABLED_ALGORITHMS:
        if a.NAME == name_clean:
            return a
    if algorithm is None:
        raise HTTPException(400, detail=f"Invalid algorithm: {name}")
    return algorithm


async def get_dataset(name: str) -> Dataset:
    catalog = await get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail="No such dataset.")
    return dataset
