from rich.table import Table
import click
from typing import Iterable
from .rt import locate_file, fetch_ntl
from ntl.search.orbital import  VIIRSNavigator
from ntl.io.rt import PRODUCT_NAMES, SOURCE_NAMES
from rich.progress import Progress
from datetime import datetime
from ntl.io import bytesto

@click.command(no_args_is_help=True)
@click.option(
    "--sat", "-s",
    "satellite", # This will be the name of the argument in your function
    type=click.Choice(VIIRSNavigator.SATELLITES, case_sensitive=False),
    multiple=False,
    help=f"Target satellite(s). One of ({','.join(VIIRSNavigator.SATELLITES)}) that produced the granule."
)

@click.option("--timestamp", "-t", "timestamp", type=str, required=True, help='Granule timestamp string as date and time. Ex: 202604152232 ')
@click.option(
    "--products",
                "-p",
                "products",
                type=click.Choice(PRODUCT_NAMES, case_sensitive=False),
                default=PRODUCT_NAMES,
                multiple=True,
                required=False,
                help=f'One or more of the products {",".join(PRODUCT_NAMES)} to download.'
    )
@click.option("-src", '--src', "source",
              type=click.Choice(SOURCE_NAMES, case_sensitive=False),
              required=False,
              help='The source {AMAZON/GOOGLE} where to search for the granules.'
              )
@click.option(
    "--dest-dir",
    "-d",           # Short option
    "dest_dir",     # Function argument name
    type=click.Path(
        exists=False,      # Set to True if you want Click to fail if the dir doesn't exist yet
        file_okay=False,   # Strictly enforce that this is a directory, not a file
        dir_okay=True,
        resolve_path=True  # Resolves relative paths (like '.') to absolute paths automatically
    ),
    default="/tmp",           # Defaults to the current working directory
    show_default=True,     # Tells the user what the default is in the --help menu
    help="Destination directory to save the downloaded the images."
)

@click.pass_obj
async def download(state, satellite:str=None, timestamp:str=None, products:Iterable[str]=None, source:str=None, dest_dir:str=None):
    """Download VIIRS imagery for a satellite and a timestamp ..."""
    table = Table(title=f"VIIRS satellites images for the night of  {timestamp} ",
                  title_style="bold yellow")

    table.add_column("Satellite", style="green", justify='center')
    table.add_column("Timestamp (UTC)", style="cyan", justify='center')
    table.add_column("Downloaded file", justify="left", style="red")
    table.add_column("File size", justify="center", style="white")
    # table.add_column("Scan Start Date and Time (UTC)", style="red", justify='center')
    with Progress(disable=False, console=state.console, transient=True) as progress:
        #progress.console.status("[bold blue]Calculating granule temporal anchors...")
        dt = datetime.strptime(timestamp, '%Y%m%d%H%M')
        found_files = await locate_file(satellite=satellite,dt=dt,source=source, products=products)
        downloaded_files = await fetch_ntl(found_paths=found_files, dst_dir=dest_dir, satellite=satellite, progress=progress)
        for local_file_path, file_size in downloaded_files.items():
            # _, file_name = os.pa
            values = satellite, timestamp, f'{local_file_path}', f'{bytesto(file_size, "m"):.2f} MB'
            table.add_row(*values)

    state.console.print(table)