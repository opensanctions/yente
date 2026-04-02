import functools
from typing import Any, AsyncIterable, Callable, Dict, Iterable, List, Optional, Union

from opentelemetry import trace

from yente import settings

_tracer = trace.get_tracer("yente.provider")


def traced(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that wraps a SearchProvider method with an OTEL span."""
    name = method.__name__

    @_tracer.start_as_current_span(f"SearchProvider.{name}")
    @functools.wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        span = trace.get_current_span()
        span.set_attribute(
            "db.system.name",
            "opensearch" if settings.INDEX_TYPE == "opensearch" else "elasticsearch",
        )
        span.set_attribute("db.operation.name", name)
        return await method(self, *args, **kwargs)

    return wrapper


class SearchProvider(object):

    async def close(self) -> None:
        raise NotImplementedError

    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        raise NotImplementedError

    async def get_all_indices(self) -> List[str]:
        """Get a list of all indices in the ElasticSearch cluster."""
        raise NotImplementedError

    async def get_alias_indices(self, alias: str) -> List[str]:
        """Get a list of indices that are aliased to the entity query alias."""
        raise NotImplementedError

    async def rollover_index(self, alias: str, next_index: str, prefix: str) -> None:
        """Remove all existing indices with a given prefix from the alias and
        add the new one."""
        raise NotImplementedError

    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        raise NotImplementedError

    async def create_index(
        self, index: str, mappings: Dict[str, Any], settings: Dict[str, Any]
    ) -> None:
        """Create a new index if it doesn't exist yet."""
        raise NotImplementedError

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        raise NotImplementedError

    async def exists_index_alias(self, alias: str, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        raise NotImplementedError

    async def check_health(self, index: str) -> bool:
        raise NotImplementedError

    async def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: Optional[int] = None,
        from_: Optional[int] = None,
        sort: Optional[List[Any]] = None,
        aggregations: Optional[Dict[str, Any]] = None,
        rank_precise: bool = False,
    ) -> Dict[str, Any]:
        """Search for entities in the index."""
        raise NotImplementedError

    async def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a document by ID using the GET API.

        Returns the document if found, None if not found.
        """
        raise NotImplementedError

    async def bulk_index(
        self, actions: Union[Iterable[Dict[str, Any]], AsyncIterable[Dict[str, Any]]]
    ) -> None:
        """Perform an iterable of bulk actions to the search index."""
        raise NotImplementedError
