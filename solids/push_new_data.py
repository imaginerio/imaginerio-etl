import dagster as dg
import subprocess

@dg.solid(
    config_schema={
        "commit":dg.Field(dg.String),
        "branch":dg.Field(dg.String)
        },
    input_defs=[
        dg.InputDefinition("commit",dagster_type=dg.Nothing)
    ]
)
def push_new_data(context):
    """
    Push data to Git submodule
    and commit changes to main
    repository
    """

    commit = context.solid_config["commit"]
    branch = context.solid_config["branch"]

    submodule_push = [
        "pwd",
        "git checkout main",
        "git add .",
        f"git commit -a -m ':card_file_box: Update {commit} data'",
        "git push",
    ]

    for command in submodule_push:
        git_cli_sub = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd="data",
        )
        output, errors = git_cli_sub.communicate()
        if "nothing" in output:
            break

    etl_push = [
        "pwd",
        f"git checkout {branch}",
        "git add data",
        "git commit -m ':card_file_box: Update submodule'",
        "git push",
    ]

    for command in etl_push:
        git_cli_etl = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd=".",
        )

        output, errors = git_cli_etl.communicate()
        if errors:
            context.log.info(f"command: {command} \noutput: {output} \nERROR: {errors}")
        else:
            context.log.info(f"command: {command} \noutput: {output}")