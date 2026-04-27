import click
from click import Parameter, Context


class BboxParamType(click.ParamType):
    name = "bbox"
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        return "<lonmin,latmin,lonmax,latmax>"
    def convert(self, value, param, ctx):
        try:
            bbox = tuple([float(x.strip()) for x in value.split(",")])
            fail = False
        except ValueError:  # ValueError raised when passing non-numbers to float()
            fail = True

        if fail or len(bbox) != 4:
            self.fail(
                f"bbox must be 4 floating point numbers separated by commas. Got '{value}'"
            )

        return bbox