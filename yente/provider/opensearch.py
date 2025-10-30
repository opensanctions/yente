from enum import StrEnum
import json
import asyncio
import logging
from typing import Any, AsyncIterable, Dict, Iterable, List, Optional, Union, cast
from opensearchpy import AsyncOpenSearch, AsyncHttpConnection, AWSV4SignerAsyncAuth
from opensearchpy.helpers import async_streaming_bulk
from opensearchpy.exceptions import NotFoundError, TransportError, ConnectionError

from yente import settings
from yente.exc import IndexNotReadyError, YenteIndexError, YenteNotFoundError
from yente.logs import get_logger
from yente.provider.base import SearchProvider

log = get_logger(__name__)
logging.getLogger("opensearch").setLevel(logging.ERROR)


class OpenSearchServiceType(StrEnum):
    ES = "es"
    AOSS = "aoss"  # Amazon OpenSearch Serverless


class OpenSearchProvider(SearchProvider):
    @classmethod
    async def create(cls) -> "OpenSearchProvider":
        """Get elasticsearch connection."""
        kwargs: Dict[str, Any] = dict(
            request_timeout=60,
            retry_on_timeout=True,
            max_retries=10,
            hosts=[settings.INDEX_URL],
            connection_class=AsyncHttpConnection,
        )
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
            try:
                es = AsyncOpenSearch(**kwargs)
                # Cluster health is not supported for Serverless
                if service_type != OpenSearchServiceType.AOSS:
                    await es.cluster.health(wait_for_status="yellow", timeout=5)
                return OpenSearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error("Cannot connect to OpenSearch: %r" % exc)
                if es is not None:
                    await es.close()
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to OpenSearch.")

    def __init__(self, client: AsyncOpenSearch) -> None:
        super().__init__()
        self.client = client

    async def close(self) -> None:
        await self.client.close()

    async def refresh(self, index: str) -> None:
        """Refresh the index to make changes visible."""
        try:
            await self.client.indices.refresh(index=index)
        except NotFoundError as nfe:
            raise YenteNotFoundError(f"Index {index} does not exist.") from nfe

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
        except TransportError as te:
            raise YenteIndexError(f"Could not get alias indices: {te}") from te

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
            raise YenteIndexError(f"Could not rollover index: {te}") from te

    async def clone_index(self, base_version: str, target_version: str) -> None:
        """Create a copy of the index with the given name."""
        if base_version == target_version:
            raise ValueError("Cannot clone an index to itself.")
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
            await self.client.indices.put_settings(
                index=base_version,
                body={"settings": {"index.blocks.read_only": False}},
            )
            log.info("Cloned index", base=base_version, target=target_version)
        except TransportError as te:
            msg = f"Could not clone index {base_version} to {target_version}: {te}"
            raise YenteIndexError(msg) from te

    async def create_index(
        self, index: str, mappings: Dict[str, Any], settings: Dict[str, Any]
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
            if "resource_already_exists_exception" in exc.error:
                return
            raise YenteIndexError(f"Could not create index: {exc}") from exc

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client.indices.delete(index=index)
        except NotFoundError:
            pass
        except TransportError as te:
            raise YenteIndexError(f"Could not delete index: {te}") from te

    async def exists_index_alias(self, alias: str, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        try:
            resp = await self.client.indices.exists_alias(name=alias, index=index)
            return bool(resp)
        except NotFoundError:
            return False
        except TransportError as te:
            raise YenteIndexError(f"Could not check index alias: {te}") from te

    async def check_health(self, index: str) -> bool:
        try:
            health = await self.client.cluster.health(index=index, timeout=5)
            return health.get("status") in ("yellow", "green")
        except NotFoundError as nfe:
            raise YenteNotFoundError(f"Index {index} does not exist.") from nfe
        except TransportError as te:
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
            async with self.query_semaphore:
                body: Dict[str, Any] = {"query": query}
                if aggregations is not None:
                    body["aggregations"] = aggregations
                if sort is not None:
                    body["sort"] = sort
                response = await self.client.search(
                    index=index,
                    size=size,
                    from_=from_,
                    body=body,
                    search_type=search_type,
                )
                return cast(Dict[str, Any], response)
        except TransportError as exc:
            if "index_not_found_exception" in exc.error:
                msg = (
                    f"Index {index} does not exist. This may be caused by a misconfiguration,"
                    " or the initial ingestion of data is still ongoing."
                )
                raise IndexNotReadyError(msg) from exc
            if "search_phase_execution_exception" in exc.error:
                raise YenteIndexError(f"Search error: {str(exc)}", status=400) from exc

            log.warning(
                f"API error {exc.status_code}: {exc.error}",
                index=index,
                query=json.dumps(query),
            )
            raise YenteIndexError(f"Could not search index: {exc}") from exc
        except (
            KeyboardInterrupt,
            OSError,
            Exception,
            asyncio.TimeoutError,
            asyncio.CancelledError,
        ) as exc:
            msg = f"Error during search: {str(exc)}"
            raise YenteIndexError(msg, status=500) from exc

    async def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a document by ID using the GET API.

        Returns the document if found, None if not found.
        """
        try:
            async with self.query_semaphore:
                response = await self.client.get(index=index, id=doc_id)
                return cast(Dict[str, Any], response)
        except NotFoundError:
            return None
        except Exception as exc:
            raise YenteIndexError(f"Error getting document: {exc}") from exc

    async def bulk_index(
        self, actions: Union[Iterable[Dict[str, Any]], AsyncIterable[Dict[str, Any]]]
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
        # We just then just raise a YenteIndexError with the first error. We could do a dance here to collect
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
            if not ok:
                raise YenteIndexError(f"Could not index entity: {item!r}")
