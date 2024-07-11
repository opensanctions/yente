from abc import abstractmethod, ABC
from contextlib import asynccontextmanager
import time
import asyncio
import warnings
from threading import Lock
from typing import cast, Any, Dict, List, AsyncGenerator, Coroutine, Tuple
from typing import AsyncIterator
from structlog.contextvars import get_contextvars
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from elasticsearch.exceptions import (
    ElasticsearchWarning,
    BadRequestError,
    NotFoundError,
)
from elasticsearch.exceptions import TransportError, ConnectionError
from followthemoney import model
from followthemoney.types.date import DateType


from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.search.mapping import (
    make_entity_mapping,
    INDEX_SETTINGS,
    NAMES_FIELD,
    NAME_PHONETIC_FIELD,
    NAME_PART_FIELD,
    NAME_KEY_FIELD,
)
from yente.search.util import (
    parse_index_name,
    construct_index_name,
)

log = get_logger(__name__)


class SearchProvider(ABC):
    pass


class ElasticSearchProvider(SearchProvider):
    @classmethod
    async def create(cls) -> "ElasticSearchProvider":
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
                es_ = es.options(request_timeout=5)
                await es_.cluster.health(wait_for_status="yellow")
                return ElasticSearchProvider(es)
            except (TransportError, ConnectionError) as exc:
                log.error("Cannot connect to ElasticSearch: %r" % exc)
                time.sleep(retry**2)

        raise RuntimeError("Could not connect to ElasticSearch.")

    def __init__(self, client: AsyncElasticsearch) -> None:
        self.client = client

    async def close(self) -> None:
        await self.client.close()

    async def list_indexes(self, alias: str = "*") -> List[str]:
        resp = await self.client.indices.get_alias(name=alias)
        return list(resp.keys())

    async def upsert_index(self, index: str) -> None:
        """
        Create an index if it does not exist. If it does exist, do nothing.
        """
        try:
            schemata = list(model.schemata.values())
            mapping = make_entity_mapping(schemata)
            await self.client.indices.create(
                index=index, mappings=mapping, settings=INDEX_SETTINGS
            )
        except BadRequestError:
            pass

    async def clone_index(self, index: str, new_index: str) -> None:
        try:
            await self._add_write_block(index)
            await self.client.indices.clone(
                index=index,
                target=new_index,
                body={
                    "settings": {"index": {"blocks": {"read_only": False}}},
                },
            )
        finally:
            await self._remove_write_block(index)

    async def delete_index(self, index: str) -> None:
        """Delete a given index if it exists."""
        try:
            await self.client.indices.delete(index=index)
        except NotFoundError:
            pass

    async def get_backing_indexes(self, name: str) -> List[str]:
        resp = await self.client.indices.get_alias(name=name)
        return list(resp.keys())

    async def index_exists(self, index: str) -> bool:
        exists = await self.client.indices.exists(index=index)
        if exists.body:
            return True
        return False

    async def rollover(self, alias: str, new_index: str, prefix: str = "") -> None:
        """
        Remove all existing indices with a given prefix from the alias and add the new one.
        """
        actions = []
        actions.append({"remove": {"index": f"{prefix}*", "alias": alias}})
        actions.append({"add": {"index": new_index, "alias": alias}})
        await self.client.indices.update_aliases(actions=actions)
        return None

    async def count(self, index: str) -> int:
        resp = await self.client.count(index=index)
        return int(resp["count"])

    async def get_alias_sources(self, alias: str) -> List[str]:
        resp = await self.client.indices.get_alias(name=alias)
        return list(resp.keys())

    async def refresh(self, index: str) -> None:
        await self.client.indices.refresh(index=index)

    async def add_alias(self, index: str, alias: str) -> None:
        await self.client.indices.put_alias(index=index, name=alias)

    async def update(
        self, entities: AsyncGenerator[Dict[str, Any], None], index_name: str
    ) -> Tuple[int, int]:
        """
        Update the index with the given entities in bulk.
        Return a tuple of the number of successful and failed operations.
        """
        resp = await async_bulk(
            client=self.client,
            actions=self._entity_iterator(entities, index_name),
            chunk_size=500,
            raise_on_error=True,
            stats_only=True,
        )
        return cast(Tuple[int, int], resp)

    async def _entity_iterator(
        self, async_entities: AsyncGenerator[Dict[str, Any], None], index: str
    ) -> AsyncGenerator[Dict[str, Any], Any]:
        async for data in async_entities:
            yield self._to_operation(data, index)


@asynccontextmanager
async def with_provider() -> AsyncIterator[ElasticSearchProvider]:
    provider = await ElasticSearchProvider.create()
    try:
        yield provider
    finally:
        await provider.close()


# Usage example
async def main():
    async with with_provider() as provider:
        indexes = await provider.list_indexes()
        print("Indexes:", indexes)


asyncio.run(main())
