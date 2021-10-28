import dagster as dg
import subprocess

@dg.solid
def pull_new_data(context):
    """
    Update Git submodule
    """
    comands = ["git", "submodule", "update", "--init", "--recursive"]
    pull = subprocess.Popen(comands)


@dg.solid(
    config_schema={
        "commit":dg.Field(dg.String),
        "branch":dg.Field(dg.String)
        },
    input_defs=[
        dg.InputDefinition("commit",dagster_type=dg.Nothing)
    ]
)