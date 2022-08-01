import asyncio
from yente.search.indexer import update_index
from yente.logs import configure_logging


async def reindex() -> None:
    await update_index()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(reindex())
