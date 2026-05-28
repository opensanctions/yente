import asyncio
import httpx
import structlog
from typing import Optional
from structlog.stdlib import BoundLogger

from yente import settings
from yente.data.manifest import Catalog, Manifest

log: BoundLogger = structlog.get_logger(__name__)
lock = asyncio.Lock()
_catalog: Optional[Catalog] = None


async def get_catalog() -> Catalog:
    global _catalog
    if _catalog is None:
        async with lock:
            if _catalog is None:
                manifest = await Manifest.load(settings.MANIFEST)
                _catalog = await Catalog.load(manifest)
    # Silence mypy, which doesn't understand the double if block:
    assert _catalog is not None, "Catalog should be initialized"
    return _catalog


async def refresh_catalog() -> None:
    global _catalog
    log.info("Refreshing manifest/catalog...", catalog=_catalog)
    try:
        manifest = await Manifest.load(settings.MANIFEST)
        _catalog = await Catalog.load(manifest)
    except httpx.HTTPError as exc:
        log.exception("Metadata fetch error (%s): %s" % (exc.request.url, exc))
    except (Exception, KeyboardInterrupt) as exc:
        log.exception("Metadata fetch error: %s" % exc)
