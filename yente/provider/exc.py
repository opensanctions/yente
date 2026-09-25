class SearchProviderError(Exception):
    """A call to the search provider failed, and a retry is not known to help."""


class SearchProviderUnavailableError(SearchProviderError):
    """The search provider cannot serve the call now. A retry can succeed."""


class SearchProviderInvalidQueryError(SearchProviderError):
    """The search provider cannot run the query it received."""


def search_error(index: str, status: int, reason: str) -> SearchProviderError:
    """Classify a failed search by the HTTP status the search provider answered.

    Only the status is read. The error names in the response body are internal
    to the search engine and change between its versions.
    """
    msg = f"Could not search index {index}: {reason}"
    if status == 400:
        return SearchProviderInvalidQueryError(msg)
    if status == 404:
        # A search only ever targets yente's own index alias, so this is never
        # about something the caller named: either the index is not built yet,
        # or a shard closed between the query and fetch phases of the search.
        msg = (
            f"{msg} This may be caused by a misconfiguration, or the initial"
            " ingestion of data is still ongoing."
        )
        return SearchProviderUnavailableError(msg)
    if status == 429 or status >= 500:
        return SearchProviderUnavailableError(msg)
    return SearchProviderError(msg)
