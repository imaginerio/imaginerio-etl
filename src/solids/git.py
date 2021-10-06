import dagster as dg
import subprocess


@dg.solid
def pull_new_data(context):
    """
    Update Git submodule
    """
    comands = ["git", "submodule", "update", "--init", "--recursive"]
    pull = subprocess.Popen(comands)


@dg.solid
def push_new_data(context):
    """
    Push data to Git submodule
    and commit changes to main
    repository
    """
    submodule_push = [
        "pwd",
        "git checkout main",
        "git add .",
        "git commit -a -m ':card_file_box: Update data'",
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
        "git checkout feature/dagster-submodule",
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
        context.log.info(
            f"command: {command} \noutput: {output} \nERRO: {errors}")
