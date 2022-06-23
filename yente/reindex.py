import asyncio
from yente.search.base import close_es
from yente.search.indexer import update_index
from yente.logs import configure_logging


async def reindex():
    await update_index(force=True)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(reindex())
