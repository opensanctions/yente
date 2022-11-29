import asyncio
import threading
from typing import Any, AsyncGenerator, Dict, List, Optional
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from elasticsearch.exceptions import BadRequestError
from followthemoney import model
from followthemoney.types import registry
from followthemoney.types.date import DateType
from followthemoney.types.name import NameType

from yente import settings
from yente.logs import get_logger
from yente.data.dataset import Dataset
from yente.data import refresh_manifest, get_catalog
from yente.search.base import get_es, close_es
from yente.search.mapping import make_entity_mapping
from yente.search.mapping import INDEX_SETTINGS
from yente.data.util import expand_dates, expand_names

log = get_logger(__name__)


async def entity_docs(
    dataset: Dataset, index: str
) -> AsyncGenerator[Dict[str, Any], None]:
    idx = 0
    async for entity in dataset.entities():
        if idx % 1000 == 0 and idx > 0:
            log.info("Index: %d entities..." % idx, index=index)
        idx += 1

        texts = entity.pop("indexText")
        data = entity.to_dict()
        data["text"] = texts
        dates = entity.get_type_values(registry.date, matchable=True)
        data[DateType.group] = expand_dates(dates)
        names = entity.get_type_values(registry.name, matchable=True)
        data[NameType.group] = expand_names(names)
        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}


async def index_entities(es: AsyncElasticsearch, dataset: Dataset, force: bool) -> None:
    """Index entities in a particular dataset, with versioning of the index."""
    # Versioning defaults to the software version instead of a data update date:
    version = f"{settings.INDEX_VERSION}{dataset.version}"
    log.info(
        "Indexing entities",
        name=dataset.name,
        url=dataset.entities_url,
        version=version,
    )
    dataset_prefix = f"{settings.ENTITY_INDEX}-{dataset.name}"
    next_index = f"{dataset_prefix}-{version}"
    exists = await es.indices.exists(index=next_index)
    if exists.body and not force:
        log.info("Index is up to date.", index=next_index)
        return

    # await es.indices.delete(index=next_index)
    log.info("Create index", index=next_index)
    try:
        schemata = list(model.schemata.values())
        mapping = make_entity_mapping(schemata)
        await es.indices.create(
            index=next_index,
            mappings=mapping,
            settings=INDEX_SETTINGS,
        )
    except BadRequestError as exc:
        log.warning(
            "Cannot create index: %s" % exc.message,
            index=next_index,
        )

    try:
        docs = entity_docs(dataset, next_index)
        await async_bulk(es, docs, yield_ok=False, stats_only=True, chunk_size=1000)
    except (KeyboardInterrupt, OSError, Exception) as exc:
        log.exception("Indexing error: %s" % exc)
        await es.indices.delete(index=next_index)
        return

    await es.indices.refresh(index=next_index)
    res = await es.indices.put_alias(index=next_index, name=settings.ENTITY_INDEX)
    if res.meta.status != 200:
        log.error("Failed to alias next index", index=next_index)
        return

    log.info("Index is now aliased to: %s" % settings.ENTITY_INDEX, index=next_index)
    indices: Any = await es.cat.indices(format="json")
    current: List[str] = [s.get("index") for s in indices]
    current = [c for c in current if c.startswith(f"{dataset_prefix}-")]
    if len(current) == 0:
        log.error("No index was created", index=next_index)
        return
    for index in current:
        if index != next_index:
            log.info("Delete other index", index=index)
            await es.indices.delete(index=index)


async def update_index(force: bool = False) -> None:
    es_ = await get_es()
    es = es_.options(request_timeout=300)
    try:
        await refresh_manifest()
        catalog = await get_catalog()
        log.info("Index update check")
        indexers = []
        for dataset in catalog.datasets:
            if dataset.load:
                indexers.append(index_entities(es, dataset, force))
        await asyncio.gather(*indexers)
        log.info("Index update complete.")
    finally:
        await es.close()
        await close_es()


def update_index_threaded(force: bool = False) -> None:
    async def update_in_thread() -> None:
        await update_index(force=force)

    thread = threading.Thread(
        target=asyncio.run,
        args=(update_in_thread(),),
        daemon=True,
    )
    thread.start()
    # asyncio.to_thread(update_index, force=force)
