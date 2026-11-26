from typing import Any

import pytest
from nomenklatura.matching.types import MatchingResult, ScoringAlgorithm

from yente.data.entity import Entity
from yente.scoring import score_results

SCORES = {"high": 0.9, "middle": 0.5, "low": 0.2}


class FixedScores(ScoringAlgorithm):
    """Scores each candidate by looking up its ID in SCORES."""

    NAME = "fixed-scores"

    @classmethod
    def compare(cls, query: Any, result: Any, config: Any) -> MatchingResult:
        return MatchingResult(score=SCORES[result.id], explanations={})


def make_entity(id_: str) -> Entity:
    return Entity.from_dict(
        {
            "id": id_,
            "schema": "Person",
            "properties": {"name": ["Jane Fixture Example"]},
            "datasets": ["test"],
        }
    )


async def score(**kwargs: Any) -> tuple[int, dict[str, bool]]:
    """Returns the number of matches, and whether each returned result matches."""
    # Candidates in ascending score order, to show the results get sorted.
    candidates = [(make_entity(id_), 1.0) for id_ in ["low", "middle", "high"]]
    matches, scored = await score_results(
        FixedScores, make_entity("query"), candidates, **kwargs
    )
    return matches, {r.id: r.match for r in scored}


@pytest.mark.asyncio
async def test_threshold():
    matches, results = await score(threshold=0.5, cutoff=0.0)
    assert results == {"high": True, "middle": True, "low": False}
    assert list(results) == ["high", "middle", "low"]
    assert matches == 2


@pytest.mark.asyncio
async def test_cutoff():
    # A score equal to the cutoff is excluded.
    matches, results = await score(threshold=0.5, cutoff=0.5)
    assert results == {"high": True}
    assert matches == 1


@pytest.mark.asyncio
async def test_limit():
    matches, results = await score(threshold=0.5, cutoff=0.0, limit=1)
    assert results == {"high": True}
    # Matches are counted before the limit is applied.
    assert matches == 2
