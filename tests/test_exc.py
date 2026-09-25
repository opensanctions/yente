import pytest

from yente.exc import YenteConfigError, YenteError
from yente.provider.exc import (
    SearchProviderError,
    SearchProviderInvalidQueryError,
    SearchProviderUnavailableError,
    search_error,
)


def test_an_error_answers_with_the_status_of_its_class() -> None:
    assert YenteError("boom").status == 500
    assert YenteConfigError("boom").status == 500


@pytest.mark.parametrize(
    "status,error_class",
    [
        (400, SearchProviderInvalidQueryError),
        # yente names no index that the caller chose, so a missing one is a
        # service that is not ready, not a resource that is not there.
        (404, SearchProviderUnavailableError),
        (429, SearchProviderUnavailableError),
        (500, SearchProviderUnavailableError),
        (503, SearchProviderUnavailableError),
        # Credentials that the index refuses are not fixed by a retry.
        (401, SearchProviderError),
        (403, SearchProviderError),
    ],
)
def test_a_failed_search_is_classified_by_its_status(
    status: int, error_class: type[SearchProviderError]
) -> None:
    assert type(search_error("idx", status, "boom")) is error_class
