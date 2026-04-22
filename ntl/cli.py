import asyncio
import logging
import inspect
import click
import uvloop
from rich.console import Console
from rich.logging import RichHandler

from ntl.rt.commands import passes

# 1. Global Instances
# We define console here so it's accessible to setup_logging and State
console = Console()
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class State:
    """Shared state object to pass console and logger between commands."""

    def __init__(self, console):
        self.console = console
        self.log = logging.getLogger("rich")  # Default logger


def setup_logging(debug: bool):
    """Configures the global logging state."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(
            console=console,
            rich_tracebacks=True,
            show_path=False
        )],
        force=True  # Required to re-configure after the group starts
    )
    return logging.getLogger("rich")


class NativeAsyncGroup(click.Group):
    """
    A Click Group that:
    1. Injects --debug into all subcommands.
    2. Automatically handles 'async def' callbacks.
    3. Re-configures logging if a subcommand uses --debug.
    """

    def add_command(self, cmd, name=None):
        # 1. Inject the --debug option
        if not any(opt.name == 'debug' for opt in cmd.params):
            debug_opt = click.Option(
                ["--debug"], is_flag=True, help="Enable debug logging for this command."
            )
            cmd.params.append(debug_opt)

        orig_callback = cmd.callback

        def wrapped_callback(*args, **kwargs):
            # 2. Handle the debug flag
            debug_val = kwargs.pop('debug', False)
            if debug_val:
                setup_logging(True)

            # 3. Handle Sync vs Async
            if inspect.iscoroutinefunction(orig_callback):
                return asyncio.run(orig_callback(*args, **kwargs))
            return orig_callback(*args, **kwargs)

        cmd.callback = wrapped_callback
        super().add_command(cmd, name)


# 2. Main Entry Point
@click.group(cls=NativeAsyncGroup, context_settings=dict(help_option_names=['-h', '--help']))
@click.option("--debug", is_flag=True, help="Enable global debug logs.")
@click.pass_context
def cli(ctx, debug):
    """RAPIDA Nighttime Lights Impact Engine

    Leverage VIIRS satellites to assess the impact of crises on the ground.
    """
    # Initialize the shared state
    ctx.obj = State(console)

    # Configure initial logging (handles ntl --debug ...)
    ctx.obj.log = setup_logging(debug)


# --- Register Commands ---
cli.add_command(passes)

if __name__ == "__main__":
    cli()