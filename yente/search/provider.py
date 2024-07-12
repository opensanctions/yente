import asyncio
from contextlib import asynccontextmanager
from typing import cast, Any, Dict, List, AsyncGenerator, Tuple
from typing import AsyncIterator
import warnings
from elasticsearch import AsyncElasticsearch, ElasticsearchWarning
from elasticsearch.helpers import async_bulk
from elasticsearch.exceptions import (
    BadRequestError,
    NotFoundError,
)
from elasticsearch.exceptions import TransportError, ConnectionError
from followthemoney import model


from yente import settings
from yente.logs import get_logger
from yente.search.mapping import (
    make_entity_mapping,
    INDEX_SETTINGS,
)

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
                es_ = es.options(request_timeout=5)
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

    async def get_all_indices(self) -> List[str]:
        indices: Any = await self.client.cat.indices(format="json")
        return [index.get("index") for index in indices]

    async def get_alias_indices(self, alias: str) -> List[str]:
        try:
            resp = await self.client.indices.get_alias(name=alias)
            return list(resp.keys())
        except NotFoundError:
            return []

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

    async def clone_index(self, base_version: str, target_version: str):
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

    async def create_index(self, index: str):
        """Create a new index with the given name."""
        log.info("Create index", index=index)
        try:
            schemata = list(model.schemata.values())
            mapping = make_entity_mapping(schemata)
            await self.client.indices.create(
                index=index,
                mappings=mapping,
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

    async def exists_index_alias(self, index: str) -> bool:
        """Check if an index exists and is linked into the given alias."""
        exists = await self.client.indices.exists_alias(
            name=settings.ENTITY_INDEX,
            index=index,
        )
        return True if exists.body else False

    async def refresh(self, index: str) -> None:
        await self.client.indices.refresh(index=index)

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


@asynccontextmanager
async def with_provider() -> AsyncIterator[SearchProvider]:
    provider = await SearchProvider.create()
    try:
        yield provider
    finally:
        await provider.close()
