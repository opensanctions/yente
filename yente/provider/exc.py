class SearchProviderError(Exception):
    """A call to the search provider failed, and a retry is not known to help."""


class SearchProviderUnavailableError(SearchProviderError):
    """The search provider cannot serve the call now. A retry can succeed."""


class SearchProviderInvalidQueryError(SearchProviderError):
    """The search provider cannot run the query it received."""


def search_error(index: str, status: int, reason: str) -> SearchProviderError:
    """Classify a failed search by the HTTP status the search provider answered."""
    msg = f"Could not search index {index}: {reason}"
    if status == 400:
        return SearchProviderInvalidQueryError(msg)
    # A search only ever targets yente's own index alias, so a 404 is never about
    # something the caller named. It usually comes from a shard that closed
    # between the query and fetch phases of the search, and a new search can
    # succeed.
    if status in (404, 429) or status >= 500:
        return SearchProviderUnavailableError(msg)
    return SearchProviderError(msg)
