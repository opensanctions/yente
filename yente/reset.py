import asyncio
from typing import Any, List
from yente.search.base import get_es
from yente.logs import configure_logging, get_logger

log = get_logger('reset')

async def reset() -> None:
    es = await get_es()
    indices: Any = await es.cat.indices(format="json")
    for index in indices:
        index_name: str = index.get('index')
        log.info("Delete index", index=index_name)
        await es.indices.delete(index=index_name)
    await es.close()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(reset())
