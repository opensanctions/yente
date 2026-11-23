import asyncio

import httpx
import structlog
from structlog.stdlib import BoundLogger

from yente import settings
from yente.data.manifest import Catalog, Manifest, check_dataset_queries

log: BoundLogger = structlog.get_logger(__name__)
lock = asyncio.Lock()
_manifest: Manifest | None = None
_catalog: Catalog | None = None


async def _load() -> None:
    global _manifest, _catalog
    manifest = await Manifest.load(settings.MANIFEST)
    catalog = await Catalog.load(manifest)
    check_dataset_queries(manifest, catalog)
    # Replace both only once they're known to be valid, so that a failed load
    # leaves the previous ones in use.
    _manifest = manifest
    _catalog = catalog


async def get_manifest() -> Manifest:
    if _manifest is None:
        async with lock:
            if _manifest is None:
                await _load()
    # Silence mypy, which doesn't understand the double if block:
    assert _manifest is not None, "Manifest should be initialized"
    return _manifest


async def get_catalog() -> Catalog:
    if _catalog is None:
        async with lock:
            if _catalog is None:
                await _load()
    # Silence mypy, which doesn't understand the double if block:
    assert _catalog is not None, "Catalog should be initialized"
    return _catalog


async def refresh_catalog() -> None:
    log.info("Refreshing manifest/catalog...", catalog=_catalog)
    try:
        await _load()
    except httpx.HTTPError as exc:
        log.exception(f"Metadata fetch error ({exc.request.url})")
    except (Exception, KeyboardInterrupt):
        log.exception("Metadata update error")
