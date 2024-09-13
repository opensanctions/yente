import json
import asyncio
import warnings
from typing import Any, Dict, List, Optional, cast
from typing import AsyncIterator
from elasticsearch import AsyncElasticsearch, ElasticsearchWarning
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch import ApiError, NotFoundError
from elasticsearch import TransportError, ConnectionError

from yente import settings
from yente.exc import IndexNotReadyError, YenteIndexError, YenteNotFoundError
from yente.logs import get_logger
from yente.search.mapping import make_entity_mapping, INDEX_SETTINGS
from yente.provider.base import SearchProvider, query_semaphore
from yente.middleware.trace_context import get_trace_context

log = get_logger(__name__)
warnings.filterwarnings("ignore", category=ElasticsearchWarning)


class ElasticSearchProvider(SearchProvider):
    @classmethod
    async def create(cls) -> "ElasticSearchProvider":
        """Get elasticsearch connection."""
        kwargs: Dict[str, Any] = dict(
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=10,
        )
        if settings.INDEX_SNIFF:
            kwargs["sniff_on_start"] = True
            kwargs["sniffer_timeout"] = 60
            kwargs["sniff_on_connection_fail"] = True
        if settings.ES_CLOUD_ID:
            log.info("Connecting to Elastic Cloud ID", cloud_id=settings.ES_CLOUD_ID)
            kwargs["cloud_id"] = settings.ES_CLOUD_ID
        else:
            kwargs["hosts"] = [settings.INDEX_URL]
        if settings.INDEX_USERNAME and settings.INDEX_PASSWORD:
            auth = (settings.INDEX_USERNAME, settings.INDEX_PASSWORD)
            kwargs["basic_auth"] = auth
        if settings.INDEX_CA_CERT:
            kwargs["ca_certs"] = settings.INDEX_CA_CERT
        for retry in range(2, 9):
            try:
                es = AsyncElasticsearch(**kwargs)
                es_ = es.options(request_timeout=15)
                await es_.cluster.health(wait_for_status="yellow")
                return ElasticSearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error("Cannot connect to ElasticSearch: %r" % exc)
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to ElasticSearch.")

    def __init__(self, client: AsyncElasticsearch) -> None:
        self._client = client

    def client(self, **kwargs: Any) -> AsyncElasticsearch:
        """Get the client with the current context."""
        if trace_context := get_trace_context():
            arg_headers = kwargs.get("headers", {})
            headers = arg_headers | (
                dict(
                    traceparent=str(trace_context.traceparent),
                    tracestate=str(trace_context.tracestate),
                )
            )
            kwargs.update(headers=headers)
        return self._client.options(**kwargs)

    async def close(self) -> None:
        await self._client.close()

    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        try:
            await self.client().indices.refresh(index=index)
        except NotFoundError as nfe:
            raise YenteNotFoundError(f"Index {index} does not exist.") from nfe

    async def get_all_indices(self) -> List[str]:
        """Get a list of all indices in the ElasticSearch cluster."""
        indices: Any = await self.client().cat.indices(format="json")
        return [index.get("index") for index in indices]

    async def get_alias_indices(self, alias: str) -> List[str]:
        """Get a list of indices that are aliased to the entity query alias."""
        try:
            resp = await self.client().indices.get_alias(name=alias)
            return list(resp.keys())
        except NotFoundError:
            return []
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not get alias indices: {te}") from te

    async def rollover_index(self, alias: str, next_index: str, prefix: str) -> None:
        """Remove all existing indices with a given prefix from the alias and
        add the new one."""
        try:
            actions = []
            actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
            actions.append({"add": {"index": next_index, "alias": alias}})
            await self.client().indices.update_aliases(actions=actions)
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not rollover index: {te}") from te

    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        if base_version == target_version:
            raise ValueError("Cannot clone an index to itself.")
        try:
            await self.client().indices.put_settings(
                index=base_version,
                settings={"index.blocks.read_only": True},
            )
            await self.delete_index(target_version)
            await self.client().indices.clone(
                index=base_version,
                target=target_version,
                body={
                    "settings": {"index": {"blocks": {"read_only": False}}},
                },
            )
            await self.client().indices.put_settings(
                index=base_version,
                settings={"index.blocks.read_only": False},
            )
            log.info("Cloned index", base=base_version, target=target_version)
        except (ApiError, TransportError) as te:
            msg = f"Could not clone index {base_version} to {target_version}: {te}"
            raise YenteIndexError(msg) from te

    async def create_index(self, index: str) -> None:
        """Create a new index with the given name."""
        log.info("Create index", index=index)
        try:
            await self.client().indices.create(
                index=index,
                mappings=make_entity_mapping(),
                settings=INDEX_SETTINGS,
            )
        except ApiError as exc:
            if exc.error == "resource_already_exists_exception":
                return
            raise YenteIndexError(f"Could not create index: {exc}") from exc

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client().indices.delete(index=index)
        except NotFoundError:
            pass
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not delete index: {te}") from te

    async def exists_index_alias(self, alias: str, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        try:
            exists = await self.client().indices.exists_alias(name=alias, index=index)
            return True if exists.body else False
        except NotFoundError:
            return False
        except (ApiError, TransportError) as te:
            raise YenteIndexError(f"Could not check index alias: {te}") from te

    async def check_health(self, index: str) -> bool:
        try:
            health = await self.client(request_timeout=5).cluster.health(
                index=index, timeout=0
            )
            return health.get("status") in ("yellow", "green")
        except NotFoundError as nfe:
            raise YenteNotFoundError(f"Index {index} does not exist.") from nfe
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
                response = await self.client().search(
                    index=index,
                    query=query,
                    size=size,
                    from_=from_,
                    sort=sort,
                    aggregations=aggregations,
                    search_type=search_type,
                )
                return cast(Dict[str, Any], response.body)
        except TransportError as te:
            log.warning(
                f"Backend connection error: {te.message}",
                errors=te.errors,
            )
            raise YenteIndexError(f"Could not connect to index: {te.message}") from te
        except ApiError as ae:
            if ae.error == "index_not_found_exception":
                msg = (
                    f"Index {index} does not exist. This may be caused by a misconfiguration,"
                    " or the initial ingestion of data is still ongoing."
                )
                raise IndexNotReadyError(msg) from ae
            if ae.error == "search_phase_execution_exception":
                raise YenteIndexError(f"Search error: {str(ae)}", status=400) from ae
            log.warning(
                f"API error {ae.status_code}: {ae.message}",
                index=index,
                query=json.dumps(query),
            )
            raise YenteIndexError(f"Could not search index: {ae}") from ae
        except (
            KeyboardInterrupt,
            OSError,
            Exception,
            asyncio.TimeoutError,
            asyncio.CancelledError,
        ) as exc:
            msg = f"Error during search: {str(exc)}"
            raise YenteIndexError(msg, status=500) from exc

    async def bulk_index(self, entities: AsyncIterator[Dict[str, Any]]) -> None:
        """Index a list of entities into the search index."""
        try:
            await async_bulk(
                self.client(),
                entities,
                chunk_size=1000,
                yield_ok=False,
                stats_only=True,
            )
        except BulkIndexError as exc:
            raise YenteIndexError(f"Could not index entities: {exc}") from exc
