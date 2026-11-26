import pytest
from fastapi import HTTPException

from yente import settings
from yente.routers.match import candidate_count, get_algorithm
from yente.routers.util import get_algorithm_by_name


@pytest.mark.parametrize("limit", [1, 5, 50, settings.MAX_MATCH_CANDIDATES, 500])
def test_candidate_count(limit):
    candidates = candidate_count(limit)
    assert candidates >= min(20, settings.MAX_RESULTS)
    assert candidates >= limit
    assert candidates <= max(settings.MAX_MATCH_CANDIDATES, limit)
    assert candidates <= settings.MAX_RESULTS


def test_get_algorithm():
    assert get_algorithm("best") is get_algorithm_by_name(settings.BEST_ALGORITHM)
    algorithm = get_algorithm("logic-v2", ["nm_number_mismatch"])
    assert algorithm.NAME == "logic-v2"


def test_get_algorithm_unknown():
    with pytest.raises(HTTPException) as exc:
        get_algorithm("nope")
    assert exc.value.status_code == 400


def test_get_algorithm_unsupported_config():
    with pytest.raises(HTTPException) as exc:
        get_algorithm("logic-v2", ["invalid_option"])
    assert exc.value.status_code == 400
    assert "invalid_option" in exc.value.detail
