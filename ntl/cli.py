import asyncio
import logging
import inspect
import click
import uvloop
from rich.console import Console
from rich.logging import RichHandler
from ntl.search.commands import search


# 1. Global Setup
console = Console()
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class State:
    def __init__(self, rich_console):
        self.console = rich_console
        self.log = logging.getLogger("rich")


def setup_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    logging.getLogger('pyorbital').setLevel(logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
        force=True
    )
    return logging.getLogger("rich")


class NativeAsyncGroup(click.Group):
    """
    Handles hierarchy, --debug injection, and robust async detection.
    """

    def add_command(self, cmd, name=None):
        # 1. Inject --debug
        if not any(opt.name == 'debug' for opt in cmd.params):
            debug_opt = click.Option(["--debug"], is_flag=True, help="Debug logs.")
            cmd.params.append(debug_opt)

        orig_callback = cmd.callback

        def wrapped_callback(*args, **kwargs):
            # 2. Re-configure Logging
            debug_val = kwargs.pop('debug', False)
            if debug_val:
                new_logger = setup_logging(True)
                ctx = click.get_current_context(silent=True)
                if ctx and ctx.obj:
                    ctx.obj.log = new_logger

            # 3. ROBUST ASYNC DETECTION
            # We 'unwrap' the function to see through decorators like @click.pass_obj
            actual_func = inspect.unwrap(orig_callback)

            if inspect.iscoroutinefunction(actual_func):
                return asyncio.run(orig_callback(*args, **kwargs))
            return orig_callback(*args, **kwargs)

        cmd.callback = wrapped_callback
        super().add_command(cmd, name)

    def group(self, *args, **kwargs):
        """Ensures sub-groups also use this class automatically."""
        kwargs.setdefault('cls', NativeAsyncGroup)
        return super().group(*args, **kwargs)


# --- CLI Hierarchy ---

@click.group(cls=NativeAsyncGroup, context_settings=dict(help_option_names=['-h', '--help']))
@click.option("--debug", is_flag=True, help="Enable global debug logs.")
@click.pass_context
def cli(ctx, debug):
    """RAPIDA Nighttime Lights Impact Engine"""
    ctx.obj = State(rich_console=console)
    ctx.obj.log = setup_logging(debug)


# # Define 'find' as a group within the hierarchy
# @cli.group(cls=NativeAsyncGroup, short_help='Find satellite passes and granules')
# def find():
#     """Commands for locating specific satellite data."""
#     pass
#
#
# # Register commands to the 'find' sub-group
#
# find.add_command(granules)

cli.add_command(search)
if __name__ == "__main__":
    cli()