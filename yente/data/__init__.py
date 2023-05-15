import asyncio
import threading
import structlog
from structlog.stdlib import BoundLogger

from yente.data.manifest import Catalog

log: BoundLogger = structlog.get_logger(__name__)
lock = asyncio.Lock()


async def get_catalog() -> Catalog:
    async with lock:
        if Catalog.instance is None:
            Catalog.instance = await Catalog.load()
    return Catalog.instance


async def refresh_catalog() -> None:
    # HACK: PyYAML is so slow that it sometimes hangs the workers, so
    # spawning a thread is unblocking.

    async def update_in_thread() -> None:
        log.info("Refreshing manifest/catalog...", ts=Catalog.loaded_at)
        Catalog.instance = await Catalog.load()

    thread = threading.Thread(
        target=asyncio.run,
        args=(update_in_thread(),),
        daemon=True,
    )
    thread.start()
