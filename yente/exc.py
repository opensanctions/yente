class YenteError(Exception):
    """Base exception for all Yente errors."""

    STATUS = 500

    def __init__(self, detail: str):
        self.detail = detail
        self.status = self.STATUS


class YenteConfigError(YenteError):
    """Errors resulting from misconfiguration of Yente."""
