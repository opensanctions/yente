from typing import Optional


class YenteError(Exception):
    """Base exception for all Yente errors."""

    def __init__(self, detail: str, status: int = 500):
        self.detail = detail
        self.status = status


class YenteIndexError(YenteError):
    """Errors resulting from the search backend being unhappy."""

    STATUS = 500

    def __init__(self, detail: str, status: int = STATUS, index: Optional[str] = None):
        super().__init__(detail, status)
        self.index = index


class IndexNotReadyError(YenteIndexError):
    """Raised when the index is not ready for searching."""

    STATUS = 503


class YenteNotFoundError(YenteIndexError):
    STATUS = 404
