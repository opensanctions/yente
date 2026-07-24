class YenteError(Exception):
    """Base exception for all Yente errors."""

    def __init__(self, detail: str, status: int = 500):
        self.detail = detail
        self.status = status


class YenteConfigError(YenteError):
    """Errors resulting from misconfiguration of Yente."""

    def __init__(self, detail: str):
        super().__init__(detail, status=500)


class YenteIndexError(YenteError):
    """Errors resulting from the search backend being unhappy."""

    STATUS = 500

    def __init__(self, detail: str, status: int = STATUS, index: str | None = None):
        super().__init__(detail, status)
        self.index = index


class IndexNotReadyError(YenteIndexError):
    """Raised when the index is not ready for searching."""

    STATUS = 503


class YenteNotFoundError(YenteIndexError):
    STATUS = 404


class ChecksumError(YenteIndexError):
    """Raised when the SHA1 checksum of a downloaded resource does not match the catalog."""

    def __init__(self, actual: str, expected: str, url: str = ""):
        detail = f"Checksum mismatch for {url!r}: got {actual!r}, expected {expected!r}"
        super().__init__(detail)
        self.actual = actual
        self.expected = expected
        self.url = url
