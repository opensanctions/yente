import asyncio
import functools
import json
import logging
from collections.abc import AsyncIterable, Callable, Iterable
from enum import StrEnum
from typing import (
    Any,
    cast,
)

from opensearchpy import AsyncHttpConnection, AsyncOpenSearch, AWSV4SignerAsyncAuth
from opensearchpy.exceptions import ConnectionError, NotFoundError, TransportError
from opensearchpy.helpers import async_streaming_bulk
from opentelemetry import trace

from yente import settings
from yente.logs import get_logger
from yente.provider.base import SearchProvider
from yente.provider.exc import (
    SearchProviderError,
    SearchProviderUnavailableError,
    search_error,
)

log = get_logger(__name__)
logging.getLogger("opensearch").setLevel(logging.ERROR)

# opensearch-py has no built-in OpenTelemetry instrumentation, so we add manual spans
# here. elasticsearch-py has had built-in OTel since 8.13, so ElasticSearchProvider
# doesn't need this.
_tracer = trace.get_tracer("yente.provider.opensearch")


def traced(method: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an OpenSearchProvider method with an OTEL span."""
    name = method.__name__

    @_tracer.start_as_current_span(f"{name}")
    @functools.wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        span = trace.get_current_span()
        span.set_attribute("db.system.name", "opensearch")
        span.set_attribute("db.operation.name", name)
        return await method(self, *args, **kwargs)

    return wrapper


class OpenSearchServiceType(StrEnum):
    ES = "es"
    AOSS = "aoss"  # Amazon OpenSearch Serverless


class OpenSearchProvider(SearchProvider):
    service_type: OpenSearchServiceType
    client: AsyncOpenSearch

    @classmethod
    async def create(cls) -> "OpenSearchProvider":
        """Get elasticsearch connection."""
        kwargs: dict[str, Any] = {
            "timeout": 60,
            "retry_on_timeout": True,
            "max_retries": 10,
            "hosts": [settings.INDEX_URL],
            "connection_class": AsyncHttpConnection,
        }
        service_type = OpenSearchServiceType.ES

        if settings.INDEX_SNIFF:
            kwargs["sniff_on_start"] = True
            kwargs["sniffer_timeout"] = 60
            kwargs["sniff_on_connection_fail"] = True
        if settings.INDEX_USERNAME and settings.INDEX_PASSWORD:
            auth = (settings.INDEX_USERNAME, settings.INDEX_PASSWORD)
            kwargs["http_auth"] = auth
        if settings.OPENSEARCH_REGION and settings.OPENSEARCH_SERVICE:
            from boto3 import Session

            service_type = OpenSearchServiceType(settings.OPENSEARCH_SERVICE)
            credentials = Session().get_credentials()
            kwargs["http_auth"] = AWSV4SignerAsyncAuth(
                credentials,
                settings.OPENSEARCH_REGION,
                service_type.value,
            )
        if settings.INDEX_CA_CERT:
            kwargs["ca_certs"] = settings.INDEX_CA_CERT
        for retry in range(2, 9):
            es: AsyncOpenSearch | None = None
            try:
                es = AsyncOpenSearch(**kwargs)
                # Cluster health is not supported for Serverless
                if service_type != OpenSearchServiceType.AOSS:
                    await es.cluster.health(wait_for_status="yellow", timeout=5)
                return OpenSearchProvider(es, service_type)
            except (TransportError, ConnectionError) as exc:
                log.error(f"Cannot connect to OpenSearch: {exc!r}")
                if es is not None:
                    await es.close()
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to OpenSearch.")

    def __init__(
        self, client: AsyncOpenSearch, service_type: OpenSearchServiceType
    ) -> None:
        super().__init__()
        self.client = client
        self.service_type = service_type

    async def close(self) -> None:
        await self.client.close()

    @traced
    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        if self.service_type == OpenSearchServiceType.AOSS:
            # AOSS doesn't support refresh
            # See https://github.com/opensearch-project/opensearch-py/issues/646
            return

        try:
            await self.client.indices.refresh(index=index)
        # The indexer refreshes an index it has just written, before it points the
        # alias at it. A missing index must stop that reindex, not publish the alias.
        except NotFoundError as nfe:
            raise SearchProviderError(f"Index {index} does not exist.") from nfe

    @traced
    async def get_all_indices(self) -> list[str]:
        """Get a list of all indices in the ElasticSearch cluster."""
        indices: Any = await self.client.cat.indices(format="json")
        return [index.get("index") for index in indices]

    @traced
    async def get_alias_indices(self, alias: str) -> list[str]:
        """Get a list of indices that are aliased to the entity query alias."""
        try:
            resp = await self.client.indices.get_alias(name=alias)
            return list(resp.keys())
        # The alias exists only after the first reindex. Until then, no index serves
        # it: callers see no indexed datasets, and /catalog lists no index versions.
        except NotFoundError:
            return []
        except TransportError as te:
            raise SearchProviderError(f"Could not get alias indices: {te}") from te

    @traced
    async def rollover_index(self, alias: str, next_index: str, prefix: str) -> None:
        """Remove all existing indices with a given prefix from the alias and
        add the new one."""
        try:
            body = {
                "actions": [
                    {"remove": {"index": f"{prefix}*", "alias": alias}},
                    {"add": {"index": next_index, "alias": alias}},
                ]
            }
            await self.client.indices.update_aliases(body=body)
        except TransportError as te:
            raise SearchProviderError(f"Could not rollover index: {te}") from te

    @traced
    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        if base_version == target_version:
            raise ValueError("Cannot clone an index to itself.")
        try:
            try:
                await self.client.indices.put_settings(
                    index=base_version,
                    body={"settings": {"index.blocks.read_only": True}},
                )
                await self.delete_index(target_version)
                await self.client.indices.clone(
                    index=base_version,
                    target=target_version,
                    body={
                        "settings": {"index": {"blocks": {"read_only": False}}},
                    },
                )
            except Exception:
                # On failure, clean up our failed clone and re-raise
                await self.delete_index(target_version)
                raise

            # Make the base index writeable again even if the clone failed, otherwise it
            # would be stuck in read-only mode and require manual intervention to fix.
            finally:
                await self.client.indices.put_settings(
                    index=base_version,
                    body={"settings": {"index.blocks.read_only": False}},
                )
            log.info("Cloned index", base=base_version, target=target_version)
        except TransportError as te:
            msg = f"Could not clone index {base_version} to {target_version}: {te}"
            raise SearchProviderError(msg) from te

    @traced
    async def create_index(
        self, index: str, mappings: dict[str, Any], settings: dict[str, Any]
    ) -> None:
        """Create a new index with the given name, mappings, and settings."""
        log.info("Create index", index=index)
        try:
            body = {
                "settings": settings,
                "mappings": mappings,
            }
            await self.client.indices.create(index=index, body=body)
        except TransportError as exc:
            # Another yente instance can create the lock or audit log index at the
            # same time, and a forced reindex writes into the index of its version
            # that already exists. So an existing index is success.
            if "resource_already_exists_exception" in exc.error:
                return
            raise SearchProviderError(f"Could not create index: {exc}") from exc

    @traced
    async def set_index_metadata(self, index: str, metadata: dict[str, Any]) -> None:
        try:
            await self.client.indices.put_mapping(index=index, body={"_meta": metadata})
        except TransportError as te:
            raise SearchProviderError(f"Could not set index metadata: {te}") from te

    @traced
    async def get_index_metadata(self, index: str) -> dict[str, Any]:
        try:
            response = await self.client.indices.get_mapping(index=index)
        except (NotFoundError, TransportError) as exc:
            raise SearchProviderError(f"Could not get index metadata: {exc}") from exc
        index_block = response.get(index, {})
        mappings = index_block.get("mappings", {})
        meta = mappings.get("_meta", {})
        return cast(dict[str, Any], meta)

    @traced
    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client.indices.delete(index=index)
        # Callers delete an index to make sure it is gone, for example before a
        # clone or during cleanup, so an index that is already gone is success.
        except NotFoundError:
            pass
        except TransportError as te:
            raise SearchProviderError(f"Could not delete index: {te}") from te

    @traced
    async def exists_index_alias(self, alias: str, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        try:
            resp = await self.client.indices.exists_alias(name=alias, index=index)
            return bool(resp)
        # A missing alias or index is a plain "no": the indexer then builds the index.
        except NotFoundError:
            return False
        except TransportError as te:
            raise SearchProviderError(f"Could not check index alias: {te}") from te

    @traced
    async def check_health(self, index: str) -> bool:
        try:
            health = await self.client.cluster.health(index=index, timeout=5)
            return health.get("status") in ("yellow", "green")
        # /readyz answers 503 while the initial ingestion has not built the index. A
        # 404 would tell a probe that the service is misconfigured.
        except NotFoundError as nfe:
            raise SearchProviderUnavailableError(
                f"Index {index} does not exist."
            ) from nfe
        # Any other failure also means the index cannot serve searches now, so
        # /readyz answers 503.
        except TransportError as te:
            log.error(f"Search status failure: {te}")
            return False

    @traced
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
            body: dict[str, Any] = {"query": query}
            if aggregations is not None:
                body["aggregations"] = aggregations
            if sort is not None:
                body["sort"] = sort
            if not track_total_hits:
                body["track_total_hits"] = False
            response = await self.client.search(
                index=index,
                size=size,
                from_=from_,
                body=body,
                search_type=search_type,
                # With several scope indices behind one alias, a lost shard would
                # otherwise answer 200 with a whole scope missing, which for a
                # screening lookup reads as a name that is not on the list.
                allow_partial_search_results=False,
            )
            return cast(dict[str, Any], response)
        # The request reached no working node, even after the transport retries.
        # The client gets a 503 and can retry.
        except ConnectionError as exc:
            log.warning(f"Backend connection error: {exc!s}")
            msg = f"Could not connect to index: {exc!s}"
            raise SearchProviderUnavailableError(msg) from exc
        except TransportError as exc:
            # status_code is 'N/A' on an error that never reached the index. A retry
            # is not known to pass then, so the client gets a 500.
            if not isinstance(exc.status_code, int):
                raise SearchProviderError(f"Could not search index: {exc!s}") from exc
            # The index answered with an error status. The status alone picks the
            # error, so the client gets a 503 or a 500, or a 400 on /search for an
            # invalid query. Only an unclassified error logs the query here: the
            # app handler logs an unavailable index, and an invalid query on
            # /search is the client's error.
            error = search_error(index, exc.status_code, str(exc))
            if type(error) is SearchProviderError:
                log.warning(
                    f"API error {exc.status_code}: {exc.error}",
                    index=index,
                    query=json.dumps(query),
                )
            raise error from exc
        # The client still gets a 500 with the same body as for other provider
        # errors.
        except (TimeoutError, OSError, Exception) as exc:
            raise SearchProviderError(f"Error during search: {exc!s}") from exc

    @traced
    async def get_document(self, index: str, doc_id: str) -> dict[str, Any] | None:
        """Get a document by ID using the GET API.

        Returns the document if found, None if not found.
        """
        try:
            response = await self.client.get(index=index, id=doc_id)
            return cast(dict[str, Any], response)
        # The lock document is missing when no yente instance holds the lock.
        except NotFoundError:
            return None
        except Exception as exc:
            raise SearchProviderError(f"Error getting document: {exc}") from exc

    @traced
    async def bulk_index(
        self, actions: Iterable[dict[str, Any]] | AsyncIterable[dict[str, Any]]
    ) -> None:
        """Perform an iterable of bulk actions to the search index."""
        # The logic in async_streaming_bulk is quite confusing and not well-documented. I tried
        # to make sense of it. The overall goal here is to deal well with 429, which indicate
        # rate limiting (important for OpenSearchServiceType.AOSS).
        #
        # Data is processed in chunks. The retry logic (max_retries and backoff) work per-chunk.
        # So each chunk is retried up to max_retries times.
        #
        # The request can fail in two ways: The whole request fails, or a single document fails.
        #
        # `raise_on_exception` controls what happens when the whole request fails. If True,
        # whole-request 429s are retried (it'll just retry the whole chunk), but if max_retries
        # is exceeded, a TransportError is raised. If False, the request will be retried
        # (actually, it's the same logic as the individual document retry logic) and eventually
        # the failed documents will be yielded as failed.
        #
        # `raise_on_error` controls what happens when a single document fails. If True, a BulkIndexError
        # is raised and no 429 retry logic is applied. If False, the failed document will be collected,
        # retried up to max_retries times, and those that still fail will be yielded as failed.
        #
        # So what we want to do here is: Set raise_on_exception=False and raise_on_error=False.
        # This will enable the maximum retry logic for both request-level and document-level 429s,
        # and when the max retries are exceeded, the documents that failed to index will be yielded as failed.
        # We just then just raise a SearchProviderError with the first error. We could do a dance here to collect
        # a few more, but for now this is good enough.
        #
        # I filed https://github.com/opensearch-project/opensearch-py/issues/964 about this mess.
        async for ok, item in async_streaming_bulk(
            self.client,
            actions,
            chunk_size=1000,
            # We don't care about successfully indexed documents
            yield_ok=False,
            # Set both to False to enable the retry logic for both request-level and document-level 429s
            # and just yield the failed documents as failed when the max retries are exceeded.
            raise_on_exception=False,
            raise_on_error=False,
            # OpenSearchServiceType.AOSS uses 429s as a rate limit, so retrying with a backoff is good.
            max_retries=5,
            initial_backoff=2,
        ):
            # Any rejected document fails the whole call. The lock depends on this:
            # the index rejects a create for a lock document that exists, so
            # acquire_lock sees that another instance holds the lock.
            if not ok:
                raise SearchProviderError(f"Could not index entity: {item!r}")
