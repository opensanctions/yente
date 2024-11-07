import click
import asyncio
from uvicorn import Config, Server

from yente import settings
from yente.app import create_app
from yente.logs import configure_logging, get_logger
from yente.search.indexer import update_index
from yente.provider import with_provider


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
            host=settings.HOST,
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
        for index in await provider.get_all_indices():
            if index.startswith(settings.INDEX_NAME):
                log.info("Delete index", index=index)
                await provider.delete_index(index=index)


@cli.command("clear-index", help="Delete everything in ElasticSearch")
def clear_index() -> None:
    configure_logging()
    asyncio.run(_clear_index())


if __name__ == "__main__":
    cli()
