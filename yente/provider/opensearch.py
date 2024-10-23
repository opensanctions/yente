import json
import asyncio
import logging
from typing import Any, Dict, List, Optional, cast
from typing import AsyncIterator
from opensearchpy import AsyncOpenSearch, AWSV4SignerAsyncAuth
from opensearchpy.helpers import async_bulk, BulkIndexError
from opensearchpy.exceptions import NotFoundError, TransportError

from yente import settings
from yente.exc import IndexNotReadyError, YenteIndexError, YenteNotFoundError
from yente.logs import get_logger
from yente.search.mapping import make_entity_mapping, INDEX_SETTINGS
from yente.provider.base import SearchProvider, query_semaphore

log = get_logger(__name__)
logging.getLogger("opensearch").setLevel(logging.ERROR)


class OpenSearchProvider(SearchProvider):
    @classmethod
    async def create(cls) -> "OpenSearchProvider":
        """Get elasticsearch connection."""
        kwargs: Dict[str, Any] = dict(
            request_timeout=60,
            retry_on_timeout=True,
            max_retries=10,
            hosts=[settings.INDEX_URL],
            # connection_class=AsyncHttpConnection,
        )
        if settings.INDEX_SNIFF:
            kwargs["sniff_on_start"] = True
            kwargs["sniffer_timeout"] = 60
            kwargs["sniff_on_connection_fail"] = True
        if settings.INDEX_USERNAME and settings.INDEX_PASSWORD:
            auth = (settings.INDEX_USERNAME, settings.INDEX_PASSWORD)
            kwargs["http_auth"] = auth
        if settings.OPENSEARCH_REGION and settings.OPENSEARCH_SERVICE:
            from boto3 import Session

            service = settings.OPENSEARCH_SERVICE.lower().strip()
            if service not in ["es", "aoss"]:
                raise RuntimeError(f"Invalid OpenSearch service: {service}")
            credentials = Session().get_credentials()
            kwargs["http_auth"] = AWSV4SignerAsyncAuth(
                credentials,
                settings.OPENSEARCH_REGION,
                settings.OPENSEARCH_SERVICE,
            )
        if settings.INDEX_CA_CERT:
            kwargs["ca_certs"] = settings.INDEX_CA_CERT
        for retry in range(2, 9):
            try:
                es = AsyncOpenSearch(**kwargs)
                await es.cluster.health(wait_for_status="yellow", timeout=5)
                return OpenSearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error("Cannot connect to OpenSearch: %r" % exc)
                await asyncio.sleep(retry**2)

        raise RuntimeError("Could not connect to OpenSearch.")

    def __init__(self, client: AsyncOpenSearch) -> None:
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
            await self.client.indices.update_aliases(body)
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

    async def create_index(self, index: str) -> None:
        """Create a new index with the given name."""
        log.info("Create index", index=index)
        try:
            body = {
                "settings": INDEX_SETTINGS,
                "mappings": make_entity_mapping(),
            }
            await self.client.indices.create(index=index, body=body)
        except TransportError as exc:
            if exc.error == "resource_already_exists_exception":
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
            async with query_semaphore:
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
        except TransportError as ae:
            if ae.error == "index_not_found_exception":
                msg = (
                    f"Index {index} does not exist. This may be caused by a misconfiguration,"
                    " or the initial ingestion of data is still ongoing."
                )
                raise IndexNotReadyError(msg) from ae
            if ae.error == "search_phase_execution_exception":
                raise YenteIndexError(f"Search error: {str(ae)}", status=400) from ae
            log.warning(
                f"API error {ae.status_code}: {ae.error}",
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
                self.client,
                entities,
                chunk_size=1000,
                yield_ok=False,
                stats_only=True,
                max_retries=3,
                initial_backoff=2,
            )
        except BulkIndexError as exc:
            raise YenteIndexError(f"Could not index entities: {exc}") from exc
