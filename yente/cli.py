import click
import asyncio
from typing import Any
from uvicorn import Config, Server

from yente import settings
from yente.app import create_app
from yente.logs import configure_logging, get_logger
from yente.search.indexer import update_index
from yente.search.provider import with_provider


log = get_logger("yente")


@click.group(help="yente API server")
def cli() -> None:
    pass


@cli.command("serve", help="Run uvicorn and serve requests")
def serve() -> None:
    app = create_app()
    server = Server(
        Config(
            app,
            host="0.0.0.0",
            port=settings.PORT,
            proxy_headers=True,
            reload=settings.DEBUG,
            # reload_dirs=[code_dir],
            # debug=settings.DEBUG,
            log_level=settings.LOG_LEVEL,
            server_header=False,
        ),
    )
    configure_logging()
    server.run()


@cli.command("reindex", help="Re-index the data if newer data is available")
@click.option("-f", "--force", is_flag=True, default=False)
def reindex(force: bool) -> None:
    configure_logging()
    asyncio.run(update_index(force=force))


async def _clear_index() -> None:
    async with with_provider() as provider:
        indices: Any = await provider.client.cat.indices(format="json")
        for index in indices:
            index_name: str = index.get("index")
            if index_name.startswith(settings.ES_INDEX):
                log.info("Delete index", index=index_name)
                await provider.client.indices.delete(index=index_name)


@cli.command("clear-index", help="Delete everything in ElasticSearch")
def clear_index() -> None:
    configure_logging()
    asyncio.run(_clear_index())


if __name__ == "__main__":
    cli()
