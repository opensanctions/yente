import asyncio
import json
import warnings
from collections.abc import AsyncIterable, Iterable
from typing import Any, cast

from elasticsearch import (
    ApiError,
    AsyncElasticsearch,
    ConnectionError,
    ConnectionTimeout,
    ElasticsearchWarning,
    NotFoundError,
    TransportError,
)
from elasticsearch.helpers import BulkIndexError, async_bulk

from yente import settings
from yente.logs import get_logger
from yente.middleware.trace_context import get_trace_context
from yente.provider.base import SearchProvider
from yente.provider.exc import (
    SearchProviderError,
    SearchProviderUnavailableError,
    search_error,
)

log = get_logger(__name__)
warnings.filterwarnings("ignore", category=ElasticsearchWarning)


class ElasticSearchProvider(SearchProvider):
    @classmethod
    async def create(cls) -> "ElasticSearchProvider":
        """Get elasticsearch connection."""
        kwargs: dict[str, Any] = {
            "request_timeout": 30,
            "retry_on_timeout": True,
            "max_retries": 10,
        }
        if settings.INDEX_SNIFF:
            kwargs["sniff_on_start"] = True
            kwargs["min_delay_between_sniffing"] = 60
            kwargs["sniff_on_node_failure"] = True
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
            es: AsyncElasticsearch | None = None
            try:
                es = AsyncElasticsearch(**kwargs)
                es_ = es.options(request_timeout=15)
                await es_.cluster.health(wait_for_status="yellow")
                return ElasticSearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error(f"Cannot connect to ElasticSearch: {exc!r}")
                if es is not None:
                    await es.close()
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to ElasticSearch.")

    def __init__(self, client: AsyncElasticsearch) -> None:
        super().__init__()
        self._client = client

    def client(self, **kwargs: Any) -> AsyncElasticsearch:
        """Get the client with the current context."""
        if trace_context := get_trace_context():
            arg_headers = kwargs.get("headers", {})
            headers = arg_headers | (
                {
                    "traceparent": trace_context.traceparent.as_header(),
                    "tracestate": trace_context.tracestate.as_header(),
                }
            )
            kwargs.update(headers=headers)
        return self._client.options(**kwargs)

    async def close(self) -> None:
        await self._client.close()

    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        try:
            await self.client().indices.refresh(index=index)
        # The indexer refreshes an index it has just written, before it points the
        # alias at it. A missing index must stop that reindex, not publish the alias.
        except NotFoundError as nfe:
            raise SearchProviderError(f"Index {index} does not exist.") from nfe

    async def get_all_indices(self) -> list[str]:
        """Get a list of all indices in the ElasticSearch cluster."""
        indices: Any = await self.client().cat.indices(format="json")
        return [index.get("index") for index in indices]

    async def get_alias_indices(self, alias: str) -> list[str]:
        """Get a list of indices that are aliased to the entity query alias."""
        try:
            resp = await self.client().indices.get_alias(name=alias)
            return list(resp.keys())
        # The alias exists only after the first reindex. Until then, no index serves
        # it: callers see no indexed datasets, and /catalog lists no index versions.
        except NotFoundError:
            return []
        except (ApiError, TransportError) as te:
            raise SearchProviderError(f"Could not get alias indices: {te}") from te

    async def rollover_index(self, alias: str, next_index: str, prefix: str) -> None:
        """Remove all existing indices with a given prefix from the alias and
        add the new one."""
        try:
            actions = []
            actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
            actions.append({"add": {"index": next_index, "alias": alias}})
            await self.client().indices.update_aliases(actions=actions)
        except (ApiError, TransportError) as te:
            raise SearchProviderError(f"Could not rollover index: {te}") from te

    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        if base_version == target_version:
            raise ValueError("Cannot clone an index to itself.")
        try:
            try:
                await self.client().indices.put_settings(
                    index=base_version,
                    settings={"index.blocks.read_only": True},
                )
                await self.delete_index(target_version)
                await self.client().indices.clone(
                    index=base_version,
                    target=target_version,
                    settings={"index": {"blocks": {"read_only": False}}},
                )
            except Exception:
                # On failure, clean up our failed clone and re-raise
                await self.delete_index(target_version)
                raise

            # Make the base index writeable again even if the clone failed, otherwise it
            # would be stuck in read-only mode and require manual intervention to fix.
            finally:
                await self.client().indices.put_settings(
                    index=base_version,
                    settings={"index.blocks.read_only": False},
                )
            log.info("Cloned index", base=base_version, target=target_version)
        except (ApiError, TransportError) as te:
            msg = f"Could not clone index {base_version} to {target_version}: {te}"
            raise SearchProviderError(msg) from te

    async def create_index(
        self, index: str, mappings: dict[str, Any], settings: dict[str, Any]
    ) -> None:
        """Create a new index with the given name, mappings, and settings."""
        log.info("Create index", index=index)
        try:
            await self.client().indices.create(
                index=index,
                mappings=mappings,
                settings=settings,
            )
        except ApiError as exc:
            # Another yente instance can create the lock or audit log index at the
            # same time, and a forced reindex writes into the index of its version
            # that already exists. So an existing index is success.
            if exc.error == "resource_already_exists_exception":
                return
            raise SearchProviderError(f"Could not create index: {exc}") from exc

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client().indices.delete(index=index)
        # Callers delete an index to make sure it is gone, for example before a
        # clone or during cleanup, so an index that is already gone is success.
        except NotFoundError:
            pass
        except (ApiError, TransportError) as te:
            raise SearchProviderError(f"Could not delete index: {te}") from te

    async def set_index_metadata(self, index: str, metadata: dict[str, Any]) -> None:
        try:
            await self.client().indices.put_mapping(index=index, meta=metadata)
        except (ApiError, TransportError) as te:
            raise SearchProviderError(f"Could not set index metadata: {te}") from te

    async def get_index_metadata(self, index: str) -> dict[str, Any]:
        try:
            response = await self.client().indices.get_mapping(index=index)
        except (NotFoundError, ApiError, TransportError) as exc:
            raise SearchProviderError(f"Could not get index metadata: {exc}") from exc
        body = response.body if hasattr(response, "body") else response
        index_block = body.get(index, {})
        mappings = index_block.get("mappings", {})
        meta = mappings.get("_meta", {})
        return cast(dict[str, Any], meta)

    async def exists_index_alias(self, alias: str, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        try:
            exists = await self.client().indices.exists_alias(name=alias, index=index)
            return bool(exists.body)
        # A missing alias or index is a plain "no": the indexer then builds the index.
        except NotFoundError:
            return False
        except (ApiError, TransportError) as te:
            raise SearchProviderError(f"Could not check index alias: {te}") from te

    async def check_health(self, index: str) -> bool:
        try:
            health = await self.client(request_timeout=5).cluster.health(
                index=index, timeout=0
            )
            return health.get("status") in ("yellow", "green")
        # /readyz answers 503 while the initial ingestion has not built the index. A
        # 404 would tell a probe that the service is misconfigured.
        except NotFoundError as nfe:
            raise SearchProviderUnavailableError(
                f"Index {index} does not exist."
            ) from nfe
        # Any other failure also means the index cannot serve searches now, so
        # /readyz answers 503.
        except (ApiError, TransportError) as te:
            log.error(f"Search status failure: {te}")
            return False

    async def search(
        self,
        index: str,
        query: dict[str, Any],
        size: int | None = None,
        from_: int | None = None,
        sort: list[Any] | None = None,
        aggregations: dict[str, Any] | None = None,
        rank_precise: bool = False,
        track_total_hits: bool = True,
    ) -> dict[str, Any]:
        """Search for entities in the index."""

        # This deals with a case in ElasticSearch where the scoring is off when two
        # indices are aliased together and have very different sizes, leading to
        # different term weightings:
        # https://discuss.elastic.co/t/querying-an-alias-throws-off-scoring-completely/351423/4
        search_type = "dfs_query_then_fetch" if rank_precise else None

        try:
            response = await self.client().search(
                index=index,
                query=query,
                size=size,
                from_=from_,
                sort=sort,
                aggregations=aggregations,
                search_type=search_type,
                # None leaves the backend default (exact count up to 10k) in place.
                track_total_hits=None if track_total_hits else False,
                # With several scope indices behind one alias, a lost shard would
                # otherwise answer 200 with a whole scope missing, which for a
                # screening lookup reads as a name that is not on the list.
                allow_partial_search_results=False,
            )
            return cast(dict[str, Any], response.body)
        # The request reached no working node, even after the transport retries.
        # The client gets a 503 and can retry.
        except (ConnectionError, ConnectionTimeout) as te:
            log.warning(
                f"Backend connection error: {te.message}",
                errors=te.errors,
            )
            msg = f"Could not connect to index: {te.message}"
            raise SearchProviderUnavailableError(msg) from te
        # Other transport failures, such as a response that cannot be decoded, are
        # not known to pass on a retry, so the client gets a 500.
        except TransportError as te:
            raise SearchProviderError(f"Could not search index: {te.message}") from te
        except ApiError as ae:
            # The alias has no index behind it until the initial ingestion builds
            # one, so the client gets a 503 and can retry.
            if ae.error == "index_not_found_exception":
                msg = (
                    f"Index {index} does not exist. This may be caused by a misconfiguration,"
                    " or the initial ingestion of data is still ongoing."
                )
                raise SearchProviderUnavailableError(msg) from ae
            # The index raises this for a query it cannot run, and for a query it
            # could not run on every shard, so only the status tells them apart:
            # the client gets a 400 on /search for an invalid query, a 503 for a
            # full queue or a lost shard, and otherwise a 500.
            if ae.error == "search_phase_execution_exception":
                raise search_error(index, ae.status_code, str(ae)) from ae
            log.warning(
                f"API error {ae.status_code}: {ae.message}",
                index=index,
                query=json.dumps(query),
            )
            raise search_error(index, ae.status_code, str(ae)) from ae
        # The client still gets a 500 with the same body as for other provider
        # errors.
        except (TimeoutError, OSError, Exception) as exc:
            raise SearchProviderError(f"Error during search: {exc!s}") from exc

    async def get_document(self, index: str, doc_id: str) -> dict[str, Any] | None:
        """Get a document by ID using the GET API.

        Returns the document if found, None if not found.
        """
        try:
            response = await self.client().get(index=index, id=doc_id)
            return cast(dict[str, Any], response.body)
        # The lock document is missing when no yente instance holds the lock.
        except NotFoundError:
            return None
        except Exception as exc:
            raise SearchProviderError(f"Error getting document: {exc}") from exc

    async def bulk_index(
        self, actions: Iterable[dict[str, Any]] | AsyncIterable[dict[str, Any]]
    ) -> None:
        """Perform an iterable of bulk actions to the search index."""
        try:
            await async_bulk(
                self.client(),
                actions,
                chunk_size=1000,
                yield_ok=False,
                stats_only=True,
            )
        # Any rejected document fails the whole call. The lock depends on this: the
        # index rejects a create for a lock document that exists, so acquire_lock
        # sees that another instance holds the lock.
        except BulkIndexError as exc:
            sample = exc.errors[:3] if exc.errors else []
            log.warning(
                f"Bulk index failed: {len(exc.errors)} document(s) rejected",
                errors=sample,
            )
            raise SearchProviderError(
                f"Could not index entities: {exc} (see log for sample errors)"
            ) from exc
