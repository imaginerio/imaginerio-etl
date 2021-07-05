import os
import dagster as dg
import datetime
from solids.utils import *


@dg.pipeline
def git_pipeline():
    # pull_new_data()
    push_new_data()


################   SENSORS   ##################

# cada hora
@dg.sensor(
    pipeline_name="git_pipeline",
    solid_selection=["push_new_data"],
    minimum_interval_seconds=140,
)
def trigger_git_push(context):
    last_mtime = float(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    processed_single = "data/input/kmls/processed_single"
    run_key = datetime.now().strftime("%d/%m/%Y%H%M%S")

    for filename in os.listdir(processed_single):
        filepath = os.path.join(processed_single, filename)
        fstats = os.stat(filepath)
        file_mtime = fstats.st_mtime

        if file_mtime > last_mtime:
            max_mtime = max(
                max_mtime, file_mtime
            )  # pq precisa disso? pode ser só: "context.update_cursor(str(file_mtime))" no final?
            yield dg.RunRequest(run_key=run_key)
            break

        else:
            continue

    context.update_cursor(str(max_mtime))


################   SCHEDULES   ##################


# @dg.schedule(
#     cron_schedule="0 19 * * FRI",  # rodar toda sexta as 19hs
#     pipeline_name="git_pipeline",
#     solid_selection=["pull_new_data"],
#     execution_timezone="America/Sao_Paulo",
# )
# def pull_new_data_weekly():
#     return {}


# from dagster import validate_run_config


# def test_my_cron_schedule():
#     run_config = pull_new_data_weekly(None)
#     assert validate_run_config(git_pipeline, run_config)


# CLI: dagster pipeline execute -f pipelines/git_pipeline.py
