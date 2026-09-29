class SearchProviderError(Exception):
    """A call to the search provider failed, and a retry is not known to help."""


class SearchProviderUnavailableError(SearchProviderError):
    """The search provider cannot serve the call now. A retry can succeed."""


class SearchProviderInvalidQueryError(SearchProviderError):
    """The search provider cannot run the query it received."""
