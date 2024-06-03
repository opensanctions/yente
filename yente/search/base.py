import time
import asyncio
import warnings
from threading import Lock
from typing import cast, Any, Dict, List, AsyncGenerator
from structlog.contextvars import get_contextvars
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from elasticsearch.exceptions import (
    ElasticsearchWarning,
    ConflictError,
    BadRequestError,
)
from elasticsearch.exceptions import TransportError, ConnectionError
from followthemoney import model
from followthemoney.types.date import DateType


from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.dataset import Dataset
from yente.search.mapping import make_entity_mapping
from yente.search.mapping import INDEX_SETTINGS
from yente.search.mapping import NAMES_FIELD, NAME_PHONETIC_FIELD
from yente.search.mapping import NAME_PART_FIELD, NAME_KEY_FIELD
from yente.data.util import expand_dates, phonetic_names
from yente.data.util import index_name_parts, index_name_keys

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = get_logger(__name__)
POOL: Dict[int, AsyncElasticsearch] = {}
query_semaphore = asyncio.Semaphore(settings.QUERY_CONCURRENCY)
index_lock = Lock()


def get_opaque_id() -> str:
    ctx = get_contextvars()
    return cast(str, ctx.get("trace_id"))


def get_es_connection() -> AsyncElasticsearch:
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
    return AsyncElasticsearch(**kwargs)


async def get_es() -> AsyncElasticsearch:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    if loop_id in POOL:
        return POOL[loop_id]

    for retry in range(2, 9):
        try:
            es = get_es_connection()
            es_ = es.options(request_timeout=5)
            await es_.cluster.health(wait_for_status="yellow")
            POOL[loop_id] = es
            return POOL[loop_id]
        except (TransportError, ConnectionError) as exc:
            log.error("Cannot connect to ElasticSearch: %r" % exc)
            time.sleep(retry**2)
    raise RuntimeError("Cannot connect to ElasticSearch")


async def close_es() -> None:
    loop = asyncio.get_running_loop()
    loop_id = hash(loop)
    es = POOL.pop(loop_id, None)
    if es is not None:
        log.info("Closing elasticsearch client")
        await es.close()


class SearchProvider:
    @classmethod
    async def create(cls):
        self = cls()
        self.client = await get_es()
        return self

    async def upsert_index(self, index: str):
        try:
            schemata = list(model.schemata.values())
            mapping = make_entity_mapping(schemata)
            await self.client.indices.create(
                index=index, mappings=mapping, settings=INDEX_SETTINGS
            )
        except BadRequestError:
            pass

    def remove_write_block(self, index: str):
        return self.client.indices.put_settings(
            index=index, settings={"index.blocks.read_only": False}
        )

    def add_write_block(self, index: str):
        return self.client.indices.put_settings(
            index=index, settings={"index.blocks.read_only": True}
        )

    def clone_index(self, index: str, new_index: str):
        return self.client.indices.clone(
            index=index,
            target=new_index,
            body={
                "settings": {"index": {"blocks": {"read_only": False}}},
            },
        )

    def delete_index(self, index: str):
        return self.client.indices.delete(index=index)

    async def up_to_date(self, index: str) -> bool:
        exists = await self.client.indices.exists(index=index)
        if exists.body:
            log.info("Index is up to date.", index=index)
            return True
        return False

    async def update(self, entities, index_name: str):
        resp = await async_bulk(
            client=self.client,
            actions=self._entity_iterator(entities, index_name),
            chunk_size=500,
            raise_on_error=True,
        )
        return resp

    async def _entity_iterator(
        self, async_entities: AsyncGenerator, index: str
    ) -> AsyncGenerator[Any, Any]:
        async for data in async_entities:
            yield self._to_operation(data, index)

    def _to_operation(self, body: Dict[str, Any], index: str) -> Dict[str, Any]:
        """
        Convert an entity to a bulk operation.
        """
        try:
            entity = body.pop("entity")
            doc_id = entity.pop("id")
        except KeyError:
            raise ValueError("No entity or ID in body.\n", body)
        match body.get("op"):
            case "ADD":
                return self._create_operation(entity, doc_id, index)
            case "MOD":
                return self._update_operation(entity, doc_id, index)
            case "DEL":
                return self._delete_operation(doc_id, index)
            case _:
                raise ValueError(f"Unknown operation type: {body.get('op')}")

    def _delete_operation(self, doc_id: str, index: str) -> Dict[str, Any]:
        return {
            "_op_type": "delete",
            "_index": index,
            "_id": doc_id,
        }

    def _update_operation(
        self, entity: Dict[str, Any], doc_id: str, index: str
    ) -> Dict[str, Any]:
        return {
            "_op_type": "update",
            "_index": index,
            "_id": doc_id,
            "doc": make_indexable(entity),
        }

    def _create_operation(
        self, entity: Dict[str, Any], doc_id: str, index: str
    ) -> Dict[str, Any]:
        return make_indexable(entity) | {
            "_op_type": "index",
            "_index": index,
            "_id": doc_id,
        }


class Index:
    def __init__(self, client: SearchProvider, dataset=None, index_name=None) -> None:
        if dataset is None and index_name is None:
            raise Exception("Dataset or index_name must be provided")
        if dataset is not None and index_name is not None:
            raise Exception("Only one of dataset or index_name must be provided")
        if dataset is not None:
            self.dataset = dataset
            self.index_name = "yente-entities-" + dataset.name
        else:
            self.index_name = index_name
        self.client = client

    def exists(self):
        return self.client.up_to_date(self.index_name)

    def upsert(self):
        return self.client.upsert_index(index=self.index_name)

    def delete(self):
        return self.client.delete_index(index=self.index_name)

    async def clone(self) -> "Index":
        try:
            await self.set_read_only()
            await self.client.clone_index(self.index_name, self.index_name + "-clone")
        finally:
            await self.set_read_write()
        return Index(self.client, index_name=self.index_name + "-clone")

    def set_read_only(self):
        return self.client.add_write_block(self.index_name)

    def set_read_write(self):
        return self.client.remove_write_block(self.index_name)

    def bulk_update(self, entity_iterator: AsyncGenerator):
        return self.client.update(entity_iterator, self.index_name)


def make_indexable(data):
    entity = Entity.from_dict(model, data)
    texts = entity.pop("indexText")
    doc = entity.to_full_dict(matchable=True)
    names: List[str] = doc.get(NAMES_FIELD, [])
    names.extend(entity.get("weakAlias", quiet=True))
    name_parts = index_name_parts(names)
    texts.extend(name_parts)
    doc[NAME_PART_FIELD] = name_parts
    doc[NAME_KEY_FIELD] = index_name_keys(names)
    doc[NAME_PHONETIC_FIELD] = phonetic_names(names)
    doc[DateType.group] = expand_dates(doc.pop(DateType.group, []))
    doc["text"] = texts
    del doc["id"]
    return doc
