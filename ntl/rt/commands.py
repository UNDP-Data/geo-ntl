from rich.table import Table
import click
from ntl.rt.orb_search import compute_passes, VIIRSNavigator
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
@click.pass_obj
def passes(state, satellites, target_date, bbox):
    #"""List the specific 85.4s data segments (granules) for a pass."""
    """List the VIIRS satellites passes for a given night and a geographic area."""


    table = Table(title=f"VIIRS Granules for {target_date.date()}", title_style="bold yellow")
    table.add_column("Satellite", style="green")
    table.add_column("Granule ID (Timestamp)", style="cyan")
    table.add_column("Start Time (UTC)", style="white")
    table.add_column("Bbox offset from SSP", justify="right", style="dim")

    with state.console.status("[bold blue]Calculating granule temporal anchors..."):
        result = compute_passes(satellites=satellites, target_date=target_date,bbox=bbox)
        if result:
            for apass in result:
                sat, t_start, ststr, offset = apass
                # In a real scenario, you might list the granule + the one before/after
                # to ensure full coverage of the bbox.
                table.add_row(
                    sat,
                    ststr,
                    t_start.strftime("%H:%M:%S.%f")[:-5],
                    str(offset)
                )

    if table.row_count == 0:
        state.console.print("[bold red]No granules found for this criteria.[/bold red]")
    else:
        state.console.print(table)
        state.console.print(f"\n[dim]Note: Each granule represents {1025 / 12:.2f}s of instrument data.[/dim]")


