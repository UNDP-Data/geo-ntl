from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn,TextColumn
import click
from ntl.search.orbital import VIIRSNavigator, search_granules, SearchMode
from ntl.utils.click_bbox import BboxParamType



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
    '--strategy',
    type=click.Choice(list(SearchMode), case_sensitive=False),
    default='all',
    show_default=True,
    help=(
        "Optimization level for pass selection: "
        "ALL: No filters. "
        "GEOM: Filter by elevation (>20°) and offset (<1500km). "
        "CMASK: Fetch NOAA Cloud Mask and return only the granule where the target bbox is mostly cloud free."
    )
)

@click.pass_obj
def search(state, satellites, target_date, bbox, strategy):
    #"""List the specific 85.4s data segments (granules) for a pass."""
    """Search VIIRS satellites granules for a given night and a geographic area."""


    table = Table(title=f"VIIRS satellites granules on {target_date.date()} covering {bbox}", title_style="bold yellow")
    table.add_column("Position", justify="center", style="white")
    table.add_column("Satellite", style="green", justify='center')
    table.add_column("Timestamp (UTC)", style="cyan", justify='center')
    #table.add_column("Scan Start Date and Time (UTC)", style="red", justify='center')
    table.add_column("Bbox offset from SSP (km)", justify="center", style="white" )
    table.add_column("Elevation above bbox (degrees)", justify="center", style="white" )
    if strategy == SearchMode.CMASK:
        table.add_column("Cloud coverage in bbox (%)", justify="center", style="white")
    table.add_column("Score (%)", justify="center", style="white")
    # with state.console.status("[bold blue]Calculating granule temporal anchors..."):
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn(), disable=False, console=state.console) as progress:
        progress.console.status("[bold blue]Calculating granule temporal anchors...")
        granules = search_granules(
            satellites=satellites, target_date=target_date,bbox=bbox,
            strategy=strategy,progress=progress)
        if granules:
            for i, granule in enumerate(granules, start=1):
                if strategy == SearchMode.CMASK:
                    values = f'{i}',granule.sat,granule.timestamp,f'{granule.offset}', f'{granule.elevation:.2f}', f'{granule.cloud_cover}',f'{granule.rank}'
                else:
                    values = f'{i}', granule.sat, granule.timestamp, f'{granule.offset}', f'{granule.elevation:.2f}', f'{granule.rank}'
                table.add_row(*values)


    if table.row_count == 0:
        state.console.print("[bold red]No granules found for this criteria.[/bold red]")
    else:
        state.console.print(table)
        state.console.print(f"\n[dim]Note: Each granule represents {1025 / 12:.2f}s of instrument data.[/dim]")


