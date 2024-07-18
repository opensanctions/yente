from asyncio import Semaphore
from typing import Any, Dict, List, Optional
from typing import AsyncIterator

from yente import settings

query_semaphore = Semaphore(settings.QUERY_CONCURRENCY)


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

    async def create_index(self, index: str) -> None:
        """Create a new index with the given name."""
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

    async def bulk_index(self, entities: AsyncIterator[Dict[str, Any]]) -> None:
        """Index a list of entities into the search index."""
        raise NotImplementedError
