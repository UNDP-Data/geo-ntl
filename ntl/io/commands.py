import os.path
from datetime import date
import click
from rich.table import Table
from typing import Iterable
from .rt import find_ntl, find_and_fetch_ntl
from ntl.search.orbital import  VIIRSNavigator
from ntl.io.rt import PRODUCT_NAMES, SOURCE_NAMES
from datetime import datetime
from ntl.utils.click_bbox import BboxParamType
@click.command(no_args_is_help=True)
@click.option(
    "-sat",
    "satellite", # This will be the name of the argument in your function
    type=click.Choice(VIIRSNavigator.SATELLITES, case_sensitive=False),
    multiple=False,
    #default=list(VIIRSNavigator.SATELLITES),
    help=f"Target satellite(s). One ({','.join(VIIRSNavigator.SATELLITES)})."
)

@click.option("-id", "granule", type=str, required=True, help='Granule ID as a timestamp string. Ex: d20260415_t2232007 ')
@click.option(
    "--products",
                "-p",
                "products",
                type=click.Choice(PRODUCT_NAMES, case_sensitive=False),
                default=PRODUCT_NAMES,
                multiple=True,
                required=False,
                help=f'One or more of the products {",".join(PRODUCT_NAMES)} fo search for.'
    )
@click.option("-src", "source",
              type=click.Choice(SOURCE_NAMES, case_sensitive=False),
              required=False,
              help='The source {AMAZON/GOOGLE} where to search for the granules.'
              )


@click.pass_obj
async def granules(state, satellite, granule, products, source):
    #"""List the specific 85.4s data segments (granules) for a pass."""
    """List the VIIRS satellites passes for a given night and a geographic area."""

    table = Table(title=f"Detected VIIRS Granules for {satellite} ", title_style="bold yellow")
    table.add_column("File", style="cyan", justify='center')
    table.add_column("Product", style="red", justify='center')
    table.add_column("File size (bytes)", justify="center", style="white")
    word = f'in {source}' if source else ''
    with state.console.status(f"[bold blue] Searching for VIIRS DNB granules {word} "):
        dt = datetime.strptime(f"{granule}00000", "d%Y%m%d_t%H%M%S%f")
        detected_files = await find_ntl(satellite=satellite, dt=dt, products=products, source=source)

        if detected_files:
            for source, files in detected_files.items():
                for path, size in files:
                    # In a real scenario, you might list the granule + the one before/after
                    # to ensure full coverage of the bbox.
                    pth, fname = os.path.split(path)
                    prod, *r = pth.split('/')

                    table.add_row(
                        fname,
                        prod.split('-')[-1],
                        str(size)
                    )

    if table.row_count == 0:
        state.console.print("[bold red]No granules found for this criteria.[/bold red]")
    else:
        state.console.print(table)
        #state.console.print(f"\n[dim]Note: Each granule represents {1025 / 12:.2f}s of instrument data.[/dim]")


@click.command(no_args_is_help=True)
@click.option(
    "--sat",
    "-s",
    "satellites", # This will be the name of the argument in your function
    type=click.Choice(VIIRSNavigator.SATELLITES, case_sensitive=False),
    multiple=True,
    default=list(VIIRSNavigator.SATELLITES),
    help=f"Target satellite(s). Use multiple times for more than one ({','.join(VIIRSNavigator.SATELLITES)})."
)
@click.option("--date", "target_date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option(
    "--bbox",
    type=BboxParamType(),
    required=True,
    help="A list of geographic coordinates."
)
@click.option(
    "--products",
                "-p",
                "products",
                type=click.Choice(PRODUCT_NAMES, case_sensitive=False),
                default=PRODUCT_NAMES,
                multiple=True,
                required=False,
                help=f'One or more of the products {",".join(PRODUCT_NAMES)} fo search for.'
    )
@click.option("-src", "source",
              type=click.Choice(SOURCE_NAMES, case_sensitive=False),
              required=False,
              help='The source {AMAZON/GOOGLE} where to search for the granules.'
              )


@click.pass_obj
async def download(
        state, satellites:Iterable[str]=None,target_date:date=None,
        bbox:Iterable[float]=None, products:Iterable[str]=None, source:str=None):
        result = compute_best_pass(satellites=satellites, target_date=target_date, bbox=bbox)
        if result:
            for apass in result:
                sat, t_start, ststr, offset = apass