from yente.exc import (
    ChecksumError,
    IndexNotReadyError,
    YenteConfigError,
    YenteError,
    YenteIndexError,
    YenteNotFoundError,
)


def test_an_error_answers_with_the_status_of_its_class() -> None:
    """A status declared on a subclass is the one it is raised with."""
    assert YenteError("boom").status == 500
    assert YenteConfigError("boom").status == 500
    assert YenteIndexError("boom").status == 500
    # An index that is not there to be searched yet is worth retrying.
    assert IndexNotReadyError("boom").status == 503
    assert YenteNotFoundError("boom").status == 404
    assert ChecksumError(actual="a", expected="b").status == 500


def test_an_error_takes_the_status_it_is_given() -> None:
    assert YenteIndexError("boom", status=400).status == 400
    assert IndexNotReadyError("boom", status=500).status == 500


def test_an_index_error_carries_the_index() -> None:
    assert YenteIndexError("boom", index="entities").index == "entities"
    assert YenteIndexError("boom").index is None
