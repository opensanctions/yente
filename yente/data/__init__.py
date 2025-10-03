import asyncio
import httpx
import structlog
from structlog.stdlib import BoundLogger

from yente.data.manifest import Catalog

log: BoundLogger = structlog.get_logger(__name__)
lock = asyncio.Lock()


async def get_catalog() -> Catalog:
    if Catalog.instance is None:
        async with lock:
            if Catalog.instance is None:
                Catalog.instance = await Catalog.load()
    # Silence mypy, which doesn't understand the double if block:
    assert Catalog.instance is not None, "Catalog should be initialized"
    return Catalog.instance


async def refresh_catalog() -> None:
    log.info("Refreshing manifest/catalog...", catalog=Catalog.instance)
    try:
        Catalog.instance = await Catalog.load()
    except httpx.HTTPError as exc:
        log.exception("Metadata fetch error (%s): %s" % (exc.request.url, exc))
    except (Exception, KeyboardInterrupt) as exc:
        log.exception("Metadata fetch error: %s" % exc)
