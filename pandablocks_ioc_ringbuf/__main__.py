import logging

import click
from pandablocks.asyncio import AsyncioClient
from .ioc import create_softioc

__all__ = ["cli"]


@click.group(invoke_without_command=True)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(
        ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], case_sensitive=False
    ),
)
@click.version_option()
@click.pass_context
def cli(ctx, log_level: str):
    """
    PandaBlocks client library command line interface.
    """
    level = getattr(logging, log_level.upper(), None)
    logging.basicConfig(format="%(levelname)s:%(message)s", level=level)

    # if no command is supplied, print the help message
    if ctx.invoked_subcommand is None:
        click.echo(cli.get_help(ctx))


@cli.command()
@click.argument("host")
@click.argument("prefix")
@click.option(
    "--buffer-max-size",
    type=int,
    default=100000,
    help="Maximum size of the ring buffer.",
)
def softioc(
    host: str,
    prefix: str,
    buffer_max_size: int,
):
    """
    Connect to the given HOST and create an IOC with the given PREFIX.
    """
    print(f"host: {host}")
    print(f"prefix: {prefix}")
    print(f"buffer_max_size: {buffer_max_size}")

    create_softioc(
        client=AsyncioClient(host),
        record_prefix=prefix,
        buffer_max_size=buffer_max_size,
    )

# test with: python -m pandablocks_ioc
if __name__ == "__main__":
    cli()
