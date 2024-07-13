import json
import asyncio
import warnings
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, cast
from typing import AsyncIterator
from elasticsearch import ApiError, AsyncElasticsearch, ElasticsearchWarning
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch.exceptions import BadRequestError, NotFoundError
from elasticsearch.exceptions import TransportError, ConnectionError

from yente import settings
from yente.exc import IndexNotReadyError, YenteIndexError
from yente.logs import get_logger
from yente.search.base import query_semaphore
from yente.search.mapping import make_entity_mapping, INDEX_SETTINGS

log = get_logger(__name__)
warnings.filterwarnings("ignore", category=ElasticsearchWarning)

# class SearchProvider(ABC):
#     pass


class SearchProvider(object):
    # FIXME: Naming this like the future interface so that we can introduce it all over
    # the app and learn about what the API should work like.

    @classmethod
    async def create(cls) -> "SearchProvider":
        """Get elasticsearch connection."""
        kwargs: Dict[str, Any] = dict(
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=10,
        )
        if settings.ES_SNIFF:
            kwargs["sniff_on_start"] = True
            kwargs["sniffer_timeout"] = 60
            kwargs["sniff_on_connection_fail"] = True
        if settings.ES_CLOUD_ID:
            log.info("Connecting to Elastic Cloud ID", cloud_id=settings.ES_CLOUD_ID)
            kwargs["cloud_id"] = settings.ES_CLOUD_ID
        else:
            kwargs["hosts"] = [settings.ES_URL]
        if settings.ES_USERNAME and settings.ES_PASSWORD:
            auth = (settings.ES_USERNAME, settings.ES_PASSWORD)
            kwargs["basic_auth"] = auth
        if settings.ES_CA_CERT:
            kwargs["ca_certs"] = settings.ES_CA_CERT
        for retry in range(2, 9):
            try:
                es = AsyncElasticsearch(**kwargs)
                es_ = es.options(request_timeout=15)
                await es_.cluster.health(wait_for_status="yellow")
                return SearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error("Cannot connect to ElasticSearch: %r" % exc)
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to ElasticSearch.")

    def __init__(self, client: AsyncElasticsearch) -> None:
        self.client = client

    async def close(self) -> None:
        await self.client.close()

    def set_trace_id(self, id: str) -> None:
        """Set the trace ID for the requests."""
        self.client = self.client.options(opaque_id=id)

    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        await self.client.indices.refresh(index=index)

    async def get_all_indices(self) -> List[str]:
        """Get a list of all indices in the ElasticSearch cluster."""
        indices: Any = await self.client.cat.indices(format="json")
        return [index.get("index") for index in indices]

    async def get_alias_indices(self, alias: str) -> List[str]:
        """Get a list of indices that are aliased to the entity query alias."""
        try:
            resp = await self.client.indices.get_alias(name=alias)
            return list(resp.keys())
        except NotFoundError:
            return []
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not get alias indices: {te}") from te

    async def rollover_index(self, alias: str, next_index: str, prefix: str) -> None:
        """Remove all existing indices with a given prefix from the alias and
        add the new one."""
        actions = []
        actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
        actions.append({"add": {"index": next_index, "alias": alias}})
        await self.client.indices.update_aliases(actions=actions)
        log.info(
            "Index is now aliased to: %s" % settings.ENTITY_INDEX, index=next_index
        )

    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        if base_version == target_version:
            raise ValueError("Cannot clone an index to itself.")
        try:
            await self.client.indices.put_settings(
                index=base_version,
                settings={"index.blocks.read_only": True},
            )
            await self.delete_index(target_version)
            await self.client.indices.clone(
                index=base_version,
                target=target_version,
                body={
                    "settings": {"index": {"blocks": {"read_only": False}}},
                },
            )
            log.info("Cloned index", base=base_version, target=target_version)
        finally:
            await self.client.indices.put_settings(
                index=base_version,
                settings={"index.blocks.read_only": False},
            )

    async def create_index(self, index: str) -> None:
        """Create a new index with the given name."""
        log.info("Create index", index=index)
        try:
            await self.client.indices.create(
                index=index,
                mappings=make_entity_mapping(),
                settings=INDEX_SETTINGS,
            )
        except BadRequestError as exc:
            if exc.error == "resource_already_exists_exception":
                return
            log.error(
                "Cannot create index: %s" % exc.message,
                index=index,
                error=exc.error,
            )

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client.indices.delete(index=index)
        except NotFoundError:
            pass
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not delete index: {te}") from te

    async def exists_index_alias(self, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        exists = await self.client.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=index,
        )
        return True if exists.body else False

    async def check_health(self, index: str) -> bool:
        try:
            es_ = self.client.options(request_timeout=5)
            health = await es_.cluster.health(index=index, timeout=0)
            return health.get("status") in ("yellow", "green")
        except (ApiError, TransportError) as te:
            log.error(f"Search status failure: {te}")
            return False

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

        # This deals with a case in ElasticSearch where the scoring is off when two
        # indices are aliased together and have very different sizes, leading to
        # different term weightings:
        # https://discuss.elastic.co/t/querying-an-alias-throws-off-scoring-completely/351423/4
        search_type = "dfs_query_then_fetch" if rank_precise else None

        try:
            async with query_semaphore:
                response = await self.client.search(
                    index=index,
                    query=query,
                    size=size,
                    from_=from_,
                    sort=sort,
                    aggregations=aggregations,
                    search_type=search_type,
                )
                print(type(response.body))
                return cast(Dict[str, Any], response.body)
        except TransportError as te:
            log.warning(
                f"Backend connection error: {te.message}",
                errors=te.errors,
            )
            raise YenteIndexError(f"Could not connect to index: {te}") from te
        except ApiError as ae:
            if ae.error == "index_not_found_exception":
                msg = (
                    f"Index not ready: {index}. This may be caused by a misconfiguration,"
                    " or the initial index is still being created."
                )
                raise IndexNotReadyError(msg) from ae
            log.warning(
                f"API error {ae.status_code}: {ae.message}",
                index=index,
                query=json.dumps(query),
            )
            raise YenteIndexError(f"Could not search index: {ae}") from ae

    async def bulk_index(self, entities: AsyncIterator[Dict[str, Any]]) -> None:
        """Index a list of entities into the search index."""
        try:
            await async_bulk(
                self.client,
                entities,
                chunk_size=1000,
                yield_ok=False,
                stats_only=True,
            )
        except BulkIndexError as exc:
            raise YenteIndexError(f"Could not index entities: {exc}") from exc


@asynccontextmanager
async def with_provider() -> AsyncIterator[SearchProvider]:
    provider = await SearchProvider.create()
    try:
        yield provider
    finally:
        await provider.close()
