import click
import asyncio
import csv
import sys
from uvicorn import Config, Server
from rich.console import Console
from rich.table import Table

from yente import settings
from yente.app import create_app
from yente.logs import configure_logging, get_logger
from yente.search.indexer import update_index
from yente.provider import with_provider
from yente.search.audit_log import (
    get_all_audit_log_messages,
)


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


@cli.command(
    "audit-log",
    help="Print all audit logs in a formatted table (default) or CSV format",
)
@click.option(
    "--output-format",
    "output_format",
    type=click.Choice(["rich", "csv"]),
    default="rich",
    help="Output format (default: rich)",
)
def audit_log(output_format: str) -> None:
    configure_logging()

    async def _audit_log() -> None:
        async with with_provider() as provider:
            logs = await get_all_audit_log_messages(provider)

            match output_format:
                case "csv":
                    writer = csv.writer(sys.stdout)
                    writer.writerow(["Timestamp", "Event Type", "Index", "Message"])
                    for log_entry in logs:
                        writer.writerow(
                            [
                                log_entry.timestamp.isoformat(),
                                log_entry.event_type.name,
                                log_entry.index,
                                log_entry.message,
                            ]
                        )
                case "rich":
                    console = Console()
                    table = Table(show_lines=True)

                    table.add_column("Timestamp", style="cyan", no_wrap=True)
                    table.add_column("Event Type", style="magenta")
                    table.add_column("Index", style="green")
                    table.add_column("Message", style="white")

                    for log_entry in logs:
                        timestamp_str = log_entry.timestamp.strftime(
                            "%Y-%m-%d %H:%M:%S.%f %Z"
                        )
                        table.add_row(
                            timestamp_str,
                            log_entry.event_type.name,
                            log_entry.index,
                            log_entry.message,
                        )

                    console.print(table)

    asyncio.run(_audit_log())


if __name__ == "__main__":
    cli()
