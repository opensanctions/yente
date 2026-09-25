class YenteError(Exception):
    """Base exception for all Yente errors."""

    def __init__(self, detail: str):
        self.detail = detail


class YenteConfigError(YenteError):
    """Errors resulting from misconfiguration of Yente."""
