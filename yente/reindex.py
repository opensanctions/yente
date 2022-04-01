import asyncio
from yente.search.base import close_es
from yente.search.indexer import update_index
from yente.logs import configure_logging


async def reindex():
    try:
        await update_index()
    finally:
        await close_es()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(reindex())
